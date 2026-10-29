/// Only commit Tools' account identity after the real client credential is written.
/// A launch failure is separate: the credential switch has already succeeded.
fn complete_desktop_switch<T>(
    inject: impl FnOnce() -> Result<(), String>,
    commit: impl FnOnce() -> Result<T, String>,
    launch_on_switch: bool,
    launch: impl FnOnce() -> Result<(), String>,
) -> Result<(T, Option<String>), String> {
    inject()?;
    let committed =
        commit().map_err(|error| format!("ANTIGRAVITY_SWITCH_STATE_SAVE_FAILED: {error}"))?;
    let launch_error = if launch_on_switch {
        launch().err()
    } else {
        None
    };
    Ok((committed, launch_error))
}

pub(crate) async fn switch_account_desktop(
    account_id: String,
    launch_on_switch: bool,
) -> Result<models::Account, String> {
    let permit =
        modules::account::try_begin_antigravity_switch(&modules::account::ANTIGRAVITY_SWITCH_GATE)?;
    modules::logger::log_info(&format!(
        "[Antigravity] 开始桌面切号: account_id={}, launch={}",
        account_id, launch_on_switch
    ));
    let account = tokio::time::timeout(
        Duration::from_secs(60),
        modules::account::prepare_account_for_injection(&account_id),
    )
    .await
    .map_err(|_| "ANTIGRAVITY_SWITCH_PREPARATION_TIMEOUT".to_string())??;

    // Version probes, process control, credential access and disk writes must not
    // occupy a Tokio runtime thread. The blocking worker owns the single-flight
    // permit, so dropping a caller cannot start a competing credential write.
    let (account, launch_error, _permit) = tokio::task::spawn_blocking(move || {
        let auth_mode = resolve_antigravity_desktop_auth_mode()?;
        if auth_mode == AntigravityDesktopAuthMode::SystemCredential {
            let (account, launch_error) = modules::antigravity_shared_switch::switch(
                &account,
                modules::antigravity_shared_switch::SwitchTarget::Desktop,
                launch_on_switch,
                crate::commands::system::antigravity_ide_uses_shared_credentials(),
            )?;
            return Ok::<_, String>((account, launch_error, permit));
        }
        if launch_on_switch {
            modules::process::ensure_antigravity_legacy_launch_path_configured()?;
        }
        let (account, launch_error) = complete_desktop_switch(
            || {
                // Persist prepared account data before changing the real credential.
                // The current-account pointer and default binding are committed later.
                modules::save_account(&account)?;
                let default_dir = legacy_antigravity_user_data_dir()?;
                if launch_on_switch {
                    let default_dir_str = default_dir.to_string_lossy().to_string();
                    modules::process::close_antigravity_legacy_instances(
                        &[default_dir_str.clone()],
                        &default_dir_str,
                        20,
                    )?;
                    let _ = modules::antigravity_legacy_instance::update_default_pid(None);

                    // 后台 language_server 不随窗口退出且持有旧账号凭据，需一并结束
                    modules::process::close_antigravity_legacy_background_processes(5)?;
                    // 锁持有者仍存活说明关闭阶段漏检，此时报错而不是删锁另起实例
                    modules::process::clear_stale_antigravity_singleton_lock(&default_dir)?;
                }
                match auth_mode {
                    AntigravityDesktopAuthMode::SystemCredential => {
                        modules::logger::log_info("[Antigravity] 使用系统凭据认证模式写入账号");
                        modules::antigravity_credential::write_antigravity_system_credential(
                            &account,
                        )?;
                        if let Ok(db_path) = legacy_antigravity_state_db_path() {
                            if let Err(e) =
                                modules::db::inject_account_token_to_path(&db_path, &account)
                            {
                                modules::logger::log_warn(&format!(
                                    "[Antigravity] 同步更新 state.vscdb 失败 (非致命): {}",
                                    e
                                ));
                            }
                        }
                    }
                    AntigravityDesktopAuthMode::LegacyStateDb => {
                        modules::logger::log_info("[Antigravity] 使用旧版 SQLite 认证模式写入账号");
                        let db_path = legacy_antigravity_state_db_path()?;
                        modules::db::inject_account_token_to_path(&db_path, &account)?;
                    }
                }
                update_antigravity_app_storage(&default_dir, &account);
                if let Ok(ide_dir) = modules::instance::get_default_user_data_dir() {
                    if ide_dir.exists() {
                        let _ = modules::instance::inject_account_to_profile_with_account(&ide_dir, &account);
                    }
                }
                Ok(())
            },
            || {
                let switched_account = modules::account::commit_switched_account(&account.id)?;
                if let Err(error) = modules::antigravity_legacy_instance::update_default_settings(
                    Some(Some(account.id.clone())),
                    None,
                    Some(false),
                ) {
                    // Preserve the UI's existing best-effort binding behavior.
                    modules::logger::log_warn(&format!(
                        "更新 Antigravity 默认实例绑定账号失败: {error}"
                    ));
                }
                Ok(switched_account)
            },
            launch_on_switch,
            || {
                let settings = modules::antigravity_legacy_instance::load_default_settings()?;
                let args = modules::process::parse_extra_args(&settings.extra_args);
                let pid = modules::process::start_antigravity_legacy_with_args("", &args)?;
                if let Err(error) =
                    modules::antigravity_legacy_instance::update_default_pid(Some(pid))
                {
                    modules::logger::log_warn(&format!(
                        "更新 Antigravity 默认实例 PID 失败: {}",
                        error
                    ));
                }
                Ok(())
            },
        )?;
        Ok::<_, String>((account, launch_error, permit))
    })
    .await
    .map_err(|error| format!("Antigravity 桌面切号任务失败: {}", error))??;

    modules::websocket::broadcast_account_switched(&account.id, &account.email);
    if let Some(error) = launch_error {
        if error.starts_with("APP_PATH_NOT_FOUND:") {
            return Err(error);
        }
        return Err(format!("账号已切换，但启动 Antigravity 失败: {}", error));
    }
    Ok(account)
}

#[cfg(test)]
#[path = "account_desktop_switch_tests.rs"]
mod desktop_switch_tests;
