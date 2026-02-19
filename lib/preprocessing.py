# preprocessing.py

import re
import jellyfish

# -----------------------------
# Name cleaning & phonetics
# -----------------------------

def clean_name(s: str) -> str:
    """Basic name cleanup: uppercase, strip, collapse spaces, remove extra punctuation."""
    if s is None:
        return None
    s = str(s).strip().upper()
    # Replace multiple spaces with one
    s = re.sub(r"\s+", " ", s)
    # Remove leading/trailing punctuation
    s = s.strip(".,;:-\"'")
    return s or None

def soundex_code(s: str) -> str:
    """Return Soundex code or None."""
    if not s:
        return None
    return jellyfish.soundex(str(s))

def metaphone_code(s: str) -> str:
    """Return Metaphone code or None."""
    if not s:
        return None
    return jellyfish.metaphone(str(s))

# -----------------------------
# Address cleaning
# -----------------------------

USPS_ABBREV = {
    "STREET": "ST",
    "ST": "ST",
    "ROAD": "RD",
    "RD": "RD",
    "AVENUE": "AVE",
    "AVE": "AVE",
    "BOULEVARD": "BLVD",
    "BLVD": "BLVD",
    "APARTMENT": "APT",
    "APT": "APT",
    "SUITE": "STE",
    "SUITE.": "STE",
    "STE": "STE",
    # extend as needed
}

def clean_street(s: str) -> str:
    """Uppercase, strip, collapse spaces, standardize a few common USPS-style abbreviations."""
    if s is None:
        return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", " ", s)
    tokens = s.split(" ")
    norm_tokens = [USPS_ABBREV.get(tok, tok) for tok in tokens]
    out = " ".join(norm_tokens).strip(".,")
    return out or None

def clean_city(s: str) -> str:
    if s is None:
        return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s or None

def clean_state(s: str) -> str:
    if s is None:
        return None
    s = str(s).strip().upper()
    return s or None

def normalize_zip5(zip_str: str) -> str:
    """Return 5-digit ZIP (strip ZIP+4 and non-digits)."""
    if zip_str is None:
        return None
    s = re.sub(r"\D", "", str(zip_str))
    if len(s) >= 5:
        return s[:5]
    return None

# -----------------------------
# NPI validation (Luhn variant)
# -----------------------------

def is_valid_npi(npi) -> bool:
    """
    Validate a 10-digit NPI using the Luhn check with prefix '80840'.
    """
    s = re.sub(r"\D", "", str(npi))
    if len(s) != 10:
        return False

    base = "80840" + s[:9]
    digits = [int(ch) for ch in base]

    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:  # double digits at even positions from right
            d = d * 2
            if d > 9:
                d -= 9
        total += d

    check_digit = (10 - (total % 10)) % 10
    return check_digit == int(s[-1])

