use flexi_logger::Logger;
pub fn init_all_utils() {
    Logger::with_env_or_str("info").start().unwrap();
}
