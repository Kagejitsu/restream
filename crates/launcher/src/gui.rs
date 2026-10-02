//! `Restream.exe`: starts `bin\restream.exe` and exits.
#![cfg_attr(windows, windows_subsystem = "windows")]
#[allow(dead_code)]
mod common;

fn main() -> std::process::ExitCode {
    match common::command("restream.exe").spawn() {
        Ok(_) => std::process::ExitCode::SUCCESS,
        Err(_) => {
            // No console to print to; surface the problem the Windows way.
            #[cfg(windows)]
            msgbox(&format!("Could not start {}", common::target("restream.exe").display()));
            std::process::ExitCode::from(2)
        }
    }
}

#[cfg(windows)]
fn msgbox(text: &str) {
    // Minimal MessageBoxW via the Win32 ABI; avoids a crate dependency.
    #[link(name = "user32")]
    unsafe extern "system" {
        fn MessageBoxW(hwnd: *mut core::ffi::c_void, text: *const u16, caption: *const u16, utype: u32) -> i32;
    }
    let wide = |s: &str| s.encode_utf16().chain(std::iter::once(0)).collect::<Vec<u16>>();
    let t = wide(text);
    let c = wide("Restream");
    unsafe { MessageBoxW(std::ptr::null_mut(), t.as_ptr(), c.as_ptr(), 0x10) };
}
