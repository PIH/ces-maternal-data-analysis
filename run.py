#! ./env/bin/python3

import argparse
from functools import partial
import itertools
from pathlib import Path
from pprint import pprint
import string

import ezcsv
from fuzzywuzzy import fuzz, process
from dateutil import parser
from unidecode import unidecode

# Constants ############################################################################

INPUT_DIR = Path(".") / "input"
CENSO_PATH = INPUT_DIR / "censo.csv"
PARTOS_PATH = INPUT_DIR / "partos-clean.csv"
REFS_PATH = INPUT_DIR / "refs-clean.csv"
COMMUNITIES_PATH = INPUT_DIR / "communities.txt"

INTERMEDIATES_DIR = Path(".") / "intermediates"
CENSO_WITH_PARTO_LINES = INTERMEDIATES_DIR / "censo-parto.csv"

OUTPUT_PATH = Path(".") / "output" / "censo-parto-refs.csv"

CENSO = "censo"
PARTOS = "partos"
REFS = "refs"
KEY_HAS_FUM = "has fum"
KEY_SUFFIX_MATCH = " match"
KEY_SUFFIX_NAME_MATCH_SCORE = " name match score"
KEY_SUFFIX_COMMUNITY_MATCH = " community match"
KEY_SUFFIX_NO_MATCH_CANDIDATES = " no match candidates based on ga"
ADDITIONAL_COLUMNS = {
    KEY_HAS_FUM: "",
    **{
        table + suffix: v
        for (table, (suffix, v)) in itertools.product(
            [PARTOS, REFS],
            [
                (KEY_SUFFIX_MATCH, 0),
                (KEY_SUFFIX_NAME_MATCH_SCORE, ""),
                (KEY_SUFFIX_COMMUNITY_MATCH, 0),
                (KEY_SUFFIX_NO_MATCH_CANDIDATES, 0),
            ],
        )
    },
}

ID = "ID"
NAME = "NAME"
FUM = "FUM"
DATE = "DATE"
COMMUNITY = "COMMUNITY"
OUTPUT = "OUTPUT"
PREFIXES = {CENSO: "CENSO-", PARTOS: "PARTOS-", REFS: "REFS-", OUTPUT: ""}
KEYS = {
    CENSO: {
        ID: "CSALMATID",
        NAME: "Prgnc_Paciente",
        FUM: "Prgnc_Fecha_de_ultima_menstruacion",
    },
    PARTOS: {
        ID: "CAMATID",
        NAME: "NOMBRE ",
        DATE: "FECHA Y HORA DE NACIMIENTO",
        COMMUNITY: "DIRECCION",
    },
    REFS: {
        ID: "ID",  # computed, see `compute_refs_id`
        NAME: "NOMBRE",
        DATE: "FECHA",
        COMMUNITY: "LUGAR DE PROCEDENCIA",
    },
}
KEYS.update(
    {
        OUTPUT: {
            ID: PREFIXES[CENSO] + KEYS[CENSO][ID],
            NAME: PREFIXES[CENSO] + KEYS[CENSO][NAME],
            FUM: PREFIXES[CENSO] + KEYS[CENSO][FUM],
        }
    }
)

DAYFIRST = "dayfirst"


# Parameters ###########################################################################

DEBUG = False
INFO = True
META = {CENSO: {DAYFIRST: True}, PARTOS: {DAYFIRST: True}, REFS: {DAYFIRST: False}}
NAME_MATCH_THRESHOLD = 90
DEFAULT_GA = 300  # if GA can't be computed, calculate as if it's this many days


# Main #################################################################################


def get_data():
    raw_censo_data = ezcsv.read_dicts(CENSO_PATH)
    raw_partos_data = ezcsv.read_dicts(PARTOS_PATH)
    raw_refs_data = ezcsv.read_dicts(REFS_PATH)
    raw_refs_data = [
        r
        for r in raw_refs_data
        if r["No."] and normalize_name(r[KEYS[REFS][NAME]]) != ""
    ]
    refs_data = []
    for row in raw_refs_data:
        last_valid_year = 2016
        ref_row_id, year = compute_refs_id(row, last_valid_year)
        last_valid_year = year
        refs_data.append(dict(ID=ref_row_id, **row))
    with open(COMMUNITIES_PATH) as f:
        communities = [normalize_name(n) for n in f.readlines()]
    return (raw_censo_data, raw_partos_data, refs_data, communities)


def compute_refs_id(refs_row, last_valid_year):
    try:
        year = parser.parse(
            refs_row[KEYS[REFS][DATE]], dayfirst=META[REFS][DAYFIRST]
        ).year
    except ValueError:
        year = last_valid_year
    return str(year) + "-" + refs_row["No."], year


def main():
    (raw_censo_data, raw_partos_data, raw_refs_data, communities) = get_data()

    censo_parto = do_partos(raw_censo_data, raw_partos_data, communities)

    do_refs(censo_parto, raw_refs_data, communities)


def do_partos(raw_censo_data, raw_partos_data, communities):

    print("Computing parto matches")
    with_parto_matches = match(raw_censo_data, raw_partos_data, CENSO, PARTOS)

    parto_community_matches = get_by_community(
        with_parto_matches, raw_partos_data, PARTOS, communities
    )

    censo_parto = with_parto_matches + parto_community_matches

    all_keys = sorted(
        list(
            set(
                (
                    [PREFIXES[CENSO] + l for l in raw_censo_data[0].keys()]
                    + [PREFIXES[PARTOS] + m for m in raw_partos_data[0].keys()]
                    + list(ADDITIONAL_COLUMNS.keys())
                )
            )
        )
    )

    ezcsv.write_dicts(
        censo_parto, CENSO_WITH_PARTO_LINES, mkdir=True, fieldnames=all_keys
    )

    return censo_parto


def do_refs(censo_parto, raw_refs_data, communities):

    print()
    print("Computing refs matches")
    with_parto_and_refs_matches = match(censo_parto, raw_refs_data, OUTPUT, REFS)

    refs_community_matches = get_by_community(
        with_parto_and_refs_matches, raw_refs_data, REFS, communities
    )

    censo_parto_refs = with_parto_and_refs_matches + refs_community_matches

    all_keys = sorted(
        list(
            set(
                (
                    list(censo_parto[0].keys())
                    + [PREFIXES[REFS] + m for m in raw_refs_data[0].keys()]
                    + list(ADDITIONAL_COLUMNS.keys())
                )
            )
        )
    )

    output = []
    for l in censo_parto_refs:
        output.append(dict(ADDITIONAL_COLUMNS, **l))

    ezcsv.write_dicts(output, OUTPUT_PATH, mkdir=True, fieldnames=all_keys)


def match(base_data, other_data, base_table, other_table):
    """
    Combines lines from `base_data` with `other_data`. Output should have one row
    for every for in `base_data`, y nada más.

    Output columns are the Censo columns prefixed with "CENSO-", the other
    data columns prefixed with other_table, and "match" and "name match score"
    from ADDITIONAL_COLUMNS.

    Parameters
    ----------
    base_data: list[dict]
        Table specified by `base_table`
    other_data: list[dict]
        Table specified by `other_table`
    base_table: str
        CENSO or OUTPUT
    other_table: str
        PARTOS or REFS

    Returns
    -------
    list[dict]
    """

    deliveries_by_other_name = {}
    for other_line in other_data:
        name = normalize_name(other_line[KEYS[other_table][NAME]])
        if name not in deliveries_by_other_name:
            deliveries_by_other_name[name] = []
        deliveries_by_other_name[name].append(other_line)

    results = []
    for base_data_line in base_data:
        results.append(
            match_for(
                base_data_line,
                other_data,
                deliveries_by_other_name,
                base_table,
                other_table,
            )
        )

    if len(results) != len(base_data):
        raise Exception(
            "Censo has {} lines but match results has {}. Should be equal.".format(
                len(base_data), len(results)
            )
        )

    return results


def match_for(
    base_data_line, other_data, deliveries_by_other_name, base_table, other_table
):
    censo_name = normalize_name(base_data_line[KEYS[base_table][NAME]])

    if not censo_name:
        return handle_no_censo_name(base_data_line, base_table)

    (fum, other_candidate_deliveries) = get_fum_and_other_candidate_deliveries(
        base_data_line, base_table, other_data, other_table
    )

    if len(other_candidate_deliveries) == 0:
        return handle_no_candidates(base_data_line, base_table, other_table)

    other_candidate_names = [
        normalize_name(r[KEYS[other_table][NAME]]) for r in other_candidate_deliveries
    ]

    if censo_name == "":
        print(
            "Got blank Censo name for Censo line using key {}:".format(
                KEYS[base_table][NAME]
            )
        )
        pprint(base_data_line)
    (other_name, name_score) = process.extractOne(
        censo_name, other_candidate_names, scorer=combo_ratio
    )
    if DEBUG:
        print(
            base_data_line[KEYS[base_table][ID]].ljust(5),
            base_data_line[KEYS[base_table][NAME]].ljust(38),
            other_name.ljust(38),
            name_score,
        )

    if name_score > NAME_MATCH_THRESHOLD:
        other_deliveries = deliveries_by_other_name[other_name]
        return handle_name_match(
            base_data_line, other_deliveries, base_table, other_table, name_score, fum
        )

    else:
        return handle_no_match(base_data_line, other_data, base_table, other_table, fum)


def handle_no_censo_name(base_data_line, base_table):
    if base_table == CENSO:
        print(
            "No censo name on line with ID {}".format(base_data_line[KEYS[CENSO][ID]])
        )
    if DEBUG:
        print(
            "No censo name on line with PARTOS ID {}".format(
                base_data_line[PREFIXES[PARTOS] + KEYS[PARTOS][ID]]
            )
        )
    return {PREFIXES[base_table] + k: v for k, v in base_data_line.items()}


def get_fum_and_other_candidate_deliveries(
    base_data_line, base_table, other_data, other_table
):
    raw_fum = base_data_line[KEYS[base_table][FUM]]
    try:
        fum = parser.parse(raw_fum, dayfirst=META[other_table][DAYFIRST])
        other_candidate_deliveries = filter_by_ga(
            fum, META[other_table][DAYFIRST], KEYS[other_table][DATE], other_data
        )
    except ValueError:
        if DEBUG:
            print(
                "Couldn't parse FUM '{}' for Censo row {}".format(
                    raw_fum, base_data_line[KEYS[base_table][ID]]
                )
            )
        fum = None
        other_candidate_deliveries = other_data
    return fum, other_candidate_deliveries


def handle_no_candidates(base_data_line, base_table, other_table):
    print(
        "No match candidates based on GA for censo line with CASALMATID "
        + base_data_line[KEYS[base_table][ID]]
    )
    return {
        **{PREFIXES[base_table] + k: v for k, v in base_data_line.items()},
        **{other_table + KEY_SUFFIX_NO_MATCH_CANDIDATES: 1},
    }


def handle_name_match(
    base_data_line, other_deliveries, base_table, other_table, name_score, fum
):

    if len(other_deliveries) > 1:
        other_delivery = sorted(
            other_deliveries,
            key=partial(ga, META[other_table][DAYFIRST], fum, KEYS[other_table][DATE]),
        )[0]
    else:
        other_delivery = other_deliveries[0]

    if INFO:
        print(
            base_data_line[KEYS[base_table][ID]].ljust(5),
            base_data_line[KEYS[base_table][NAME]].ljust(38),
            other_delivery[KEYS[other_table][NAME]].ljust(38),
            name_score,
        )

    return {
        **{PREFIXES[base_table] + k: v for k, v in base_data_line.items()},
        **{PREFIXES[other_table] + k: v for k, v in other_delivery.items()},
        **{
            other_table + KEY_SUFFIX_MATCH: 1,
            other_table + KEY_SUFFIX_NAME_MATCH_SCORE: name_score,
            KEY_HAS_FUM: int(bool(fum)),
        },
    }


def handle_no_match(base_data_line, other_data, base_table, other_table, fum):
    if DEBUG:
        print(
            "No match for ",
            base_data_line[KEYS[base_table][ID]].ljust(5),
            base_data_line[KEYS[base_table][NAME]].ljust(38),
        )
    return {
        **{PREFIXES[base_table] + k: v for k, v in base_data_line.items()},
        **{PREFIXES[other_table] + k: "" for k in other_data[0].keys()},
        **{KEY_HAS_FUM: int(bool(fum))},
    }


def get_by_community(matched_data, other_data, table, communities):
    """
    Parameters
    ----------
    matched_data: list[dict]
        Keys from tables are prefixed by CENSO-, PARTO-, REFS-
    other_data: list[dict]
        Table specified by `table`
    table: str
        CENSO, PARTOS, REFS, or OUTPUT
    communities: list[str]
    """

    key_matched_id = PREFIXES[table] + KEYS[table][ID]
    if KEYS[table][ID] not in other_data[0]:
        print(
            "{} not found in other_data keys: {}".format(
                KEYS[table][ID], list(other_data[0].keys())
            )
        )
    unmatched_refs = [
        r
        for r in other_data
        if r[KEYS[table][ID]]
        not in {s[key_matched_id] for s in matched_data if key_matched_id in s}
    ]
    refs_from_our_communities = [
        r
        for r in unmatched_refs
        if normalize_name(r[KEYS[table][COMMUNITY]]) in communities
    ]
    community_matches = [
        {
            **{k: "" for k in matched_data[0].keys()},
            **{PREFIXES[table] + k: v for k, v in p.items()},
            **{table + KEY_SUFFIX_COMMUNITY_MATCH: 1},
        }
        for p in refs_from_our_communities
    ]
    return community_matches


def ga(fum, dayfirst, date_key, delivery_line):
    """
    Parameters
    ----------
        fum: datetime
        dayfirst: bool
        date_key: str
        delivery_line: dict
    """
    try:
        data_date = parser.parse(delivery_line[date_key], dayfirst=dayfirst)
        return (data_date - fum).days
    except:
        return DEFAULT_GA


def filter_by_ga(fum, dayfirst, date_key, delivery_data):
    """
    Parameters
    ----------
        fum: datetime
        dayfirst: bool
        date_key: str
        delivery_data: list[dict]
    """
    results = []
    for r in delivery_data:
        try:
            data_date = parser.parse(r[date_key], dayfirst=dayfirst)
            ga = data_date - fum
            if 365 > ga.days > 0:
                results.append(r)
        except ValueError:
            results.append(r)
    return results


def normalize_name(name):
    return unidecode(remove_punctuation(name.lower().strip()))


def remove_punctuation(name):
    return name.translate(str.maketrans("", "", string.punctuation))


def validate_manual_matches():
    """ Not part of the main program. This just checks how the manually-identified
        score in the current matching algorithm. """

    (raw_censo_data, raw_partos_data, raw_refs_data, communities) = get_data()

    censo_has_match = [
        (l["CAMATID"], normalize_name(l["Prgnc_Paciente"]))
        for l in raw_censo_data
        if l["CAMATID"].strip() != ""
    ]

    parto_by_camatid = {p["CAMATID"]: p for p in raw_partos_data}
    parto_manual_matches = []
    results = []
    for (cid, cname) in censo_has_match:
        try:
            cid_int = cid.replace("CAMAT-", "")
            _ = int(cid_int)
            parto_manual_match = (
                cid_int,
                normalize_name(parto_by_camatid[cid_int]["NOMBRE "]),
            )
            parto_manual_matches.append(parto_manual_match)
            name_score = combo_ratio(normalize_name(cname), parto_manual_match[1])
            results.append(
                [parto_manual_match[0], cname, parto_manual_match[1], name_score]
            )
        except ValueError:
            print("Couldn't find match for {}: {}".format(cid, cname))
    results.sort(key=lambda a: a[3])
    for r in results:
        print(r[0].ljust(6), r[1].ljust(38), r[2].ljust(38), ":", r[3])


def combo_ratio(str1, str2):
    return int(
        fuzz.token_set_ratio(str1, str2) * 0.7 + fuzz.token_sort_ratio(str1, str2) * 0.3
    )


if __name__ == "__main__":
    main()
