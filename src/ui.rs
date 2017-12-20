


pub struct Progress {
    pub n: usize,
}


impl Progress {
    pub fn new() -> Progress {
        Progress { n: 0 }
    }

    pub fn progress(&mut self) {
        self.n += 1;
        self.show(false);
    }

    pub fn complete(&self) {
        self.show(true);
    }

    fn show(&self, last: bool) {
        let just = self.n % 100 == 0;
        if last ^ just {
            eprintln!("{} rows", self.n);
        }
    }
}
