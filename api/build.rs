fn main() {
    // sqlx::migrate! embeds the directory; a new migration file must trigger a rebuild.
    println!("cargo:rerun-if-changed=../migrations");
}
