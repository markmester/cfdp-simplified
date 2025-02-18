use std::str::FromStr;

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Available actions which can be taken when a fault has occurred.
pub enum FaultHandlerAction {
    /// Ignore the fault.
    Ignore,
    /// Halt the transaction immediately. Do not wait for handshakes.
    Abandon,
}
impl FromStr for FaultHandlerAction {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.trim().to_lowercase().as_str() {
            "ignore" => Ok(Self::Ignore),
            "abandon" => Ok(Self::Abandon),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("Ignore", FaultHandlerAction::Ignore)]
    #[case("Abandon", FaultHandlerAction::Abandon)]
    #[case("IGNORE", FaultHandlerAction::Ignore)]
    #[case("ABANDON", FaultHandlerAction::Abandon)]
    fn fault_action(#[case] input: &str, #[case] action: FaultHandlerAction) {
        assert_eq!(action, FaultHandlerAction::from_str(input).unwrap())
    }

    #[test]
    fn fault_error() {
        assert!(FaultHandlerAction::from_str("Hello, World").is_err())
    }
}
