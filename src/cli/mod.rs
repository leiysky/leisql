use std::io::{BufRead, Write};

use crate::{core::SQLError, sql::Session};

enum Command {
    CursorMove,
}

pub struct CliApp<I: BufRead, O: Write> {
    session: Session,

    input: I,
    output: O,
}

impl<I: BufRead, O: Write> CliApp<I, O> {
    pub fn new(session: Session, input: I, output: O) -> Self {
        Self {
            session,
            input,
            output,
        }
    }

    pub fn run(&mut self) -> Result<(), SQLError> {
        self.bootstrap()?;

        let mut line_buf = String::new();
        loop {
            self.prompt()?;
            line_buf.clear();
            self.input.read_line(&mut line_buf).unwrap();
            let result = self.handle_line(&line_buf).unwrap();
            self.print(&result).unwrap();
            self.print("\n").unwrap();
        }
    }

    fn bootstrap(&mut self) -> Result<(), SQLError> {
        let welcome = "Welcome to leisql!\n";
        self.print(welcome)?;
        Ok(())
    }

    fn prompt(&mut self) -> Result<(), SQLError> {
        self.output.write_all(b"you=# ").unwrap();
        self.output.flush().unwrap();
        Ok(())
    }

    fn handle_line(&mut self, line: &str) -> Result<String, SQLError> {
        let result = self.session.execute(line).unwrap_or_else(|e| e.to_string());
        Ok(result)
    }

    fn print(&mut self, string: &str) -> Result<(), SQLError> {
        self.output.write_all(string.as_bytes()).unwrap();
        self.output.flush().unwrap();
        Ok(())
    }
}
