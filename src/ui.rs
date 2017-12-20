



pub fn progress(n: usize, last: bool) {
    let just = n % 100 == 0;
    if last ^ just {
        eprintln!("{} rows", n);
    }
}
