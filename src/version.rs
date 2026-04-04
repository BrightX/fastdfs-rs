use std::fmt::{Debug, Display, Formatter};

#[derive(PartialEq, PartialOrd, Eq, Ord, Hash, Copy, Clone)]
#[repr(u16)]
pub enum Version {
    V504 = 504,
    V505 = 505,
    V508 = 508,
    V509 = 509,
    V510 = 510,
    V511 = 511,
    V512 = 512,
    V600 = 600,
    V601 = 601,
    V602 = 602,
    V603 = 603,
    V604 = 604,
    V605 = 605,
    V606 = 606,
    V607 = 607,
    V608 = 608,
    V609 = 609,
    V691 = 691,
    V692 = 692,
    V693 = 693,
    V694 = 694,
    V695 = 695,
    V6100 = 6100,
    V6110 = 6110,
    V6120 = 6120,
    V6121 = 6121,
    V6122 = 6122,
    V6123 = 6123,
    V6124 = 6124,
    V6130 = 6130,
    V6132 = 6132,
    V6150 = 6150,
    V6151 = 6151,
    V6152 = 6152,
    V6153 = 6153,
    V6154 = 6154,
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let v = match self {
            Version::V504 => "V5.04",
            Version::V505 => "V5.05",
            Version::V508 => "V5.08",
            Version::V509 => "V5.09",
            Version::V510 => "V5.10",
            Version::V511 => "V5.11",
            Version::V512 => "V5.12",
            Version::V600 => "V6.00",
            Version::V601 => "V6.01",
            Version::V602 => "V6.02",
            Version::V603 => "V6.03",
            Version::V604 => "V6.04",
            Version::V605 => "V6.05",
            Version::V606 => "V6.06",
            Version::V607 => "V6.07",
            Version::V608 => "V6.08",
            Version::V609 => "V6.09",
            Version::V691 => "V6.9.1",
            Version::V692 => "V6.9.2",
            Version::V693 => "V6.9.3",
            Version::V694 => "V6.9.4",
            Version::V695 => "V6.9.5",
            Version::V6100 => "V6.10.0",
            Version::V6110 => "V6.11.0",
            Version::V6120 => "V6.12.0",
            Version::V6121 => "V6.12.1",
            Version::V6122 => "V6.12.2",
            Version::V6123 => "V6.12.3",
            Version::V6124 => "V6.12.4",
            Version::V6130 => "V6.13.0",
            Version::V6132 => "V6.13.2",
            Version::V6150 => "V6.15.0",
            Version::V6151 => "V6.15.1",
            Version::V6152 => "V6.15.2",
            Version::V6153 => "V6.15.3",
            Version::V6154 => "V6.15.4",
        };

        write!(f, "{}", v)
    }
}

impl Debug for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Version {
    #[inline]
    pub fn latest() -> Self {
        Version::V6154
    }

    #[inline]
    pub fn oldest() -> Self {
        Version::V504
    }
}
