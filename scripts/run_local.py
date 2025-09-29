from os import name, path, system
from sys import path as sys_path

# Add the src directory to the Python path
ROOT_DIR = path.abspath(path.join(path.dirname(__file__), ".."))
SRC_DIR = path.join(ROOT_DIR, "src")
sys_path.insert(0, SRC_DIR)

from pipes.data_extraction.dwh_to_s3_extract import DWHToS3Extract
from pipes.dimensions.rpd_date import RPDDate
from pipes.facts.fact_inventory_snapshot import FactInventorySnapshot

debug = False


def toggle_debug_mode():
    global debug
    debug = not debug


def clear():
    system("cls" if name == "nt" else "clear")


def main():
    options = {
        "e_dwh": (
            "extract - dwh_to_s3",
            DWHToS3Extract,
            ["g_all_extract"],
        ),
        "d_d": (
            "dim - rpd_date",
            RPDDate,
            ["g_all_dims", "g_all_dwh"],
        ),
        "f_i": (
            "fact - inventory",
            FactInventorySnapshot,
            ["g_all_facts", "g_all_dwh"],
        ),
        "d": (
            "Toggle Debug Mode",
            toggle_debug_mode,
            [],
        ),
        "x": (
            "Exit",
            exit,
            [],
        ),
    }

    clear()

    while True:
        print(
            "Debug mode: {0}".format(
                "\x1b[6;30;42m" + "enabled" + "\x1b[0m"
                if debug
                else "\x1b[6;30;41m" + "disabled" + "\x1b[0m"
            )
        )
        print("\nOptions:")
        print("\t[Key]\t\t[Description]")
        for key, (description, _, _) in options.items():
            print(f"\t{key}\t\t{description}")

        # Display available groups
        groups = {
            group for _, _, group_list in options.values() for group in group_list
        }
        for group in sorted(groups):
            print(f"\t{group}\tRun group")

        choices = input(
            "\nPlease enter the key(s) of the option(s) you would like to run: "
        )
        choices = choices.replace(",", " ").split()

        valid_choices = [
            choice for choice in choices if choice in options or choice in groups
        ]

        if not valid_choices:
            clear()
            print("\x1b[0;33;40m" + "Invalid options. Please try again." + "\x1b[0m")
            continue

        if any(choice in valid_choices for choice in ("d", "x")):
            if len(valid_choices) == 1:
                _, action, _ = options[valid_choices[0]]
                clear()
                action()
                continue
            else:
                clear()
                print(
                    "\x1b[0;33;40m"
                    + "The 'debug' and 'exit' options must be used alone. Please enter it separately."
                    + "\x1b[0m"
                )
                continue

        clear()
        for choice in valid_choices:
            if choice in groups:  # Handle group options
                for key, (_, _, group_list) in options.items():
                    if choice in group_list:
                        print(f"\nRunning {key}...")
                        _, action, _ = options[key]
                        action(debug=debug)({}, {})
            else:  # Handle individual options
                print(f"\nRunning {choice}...")
                _, action, _ = options[choice]
                action(debug=debug)({}, {})

        return


if __name__ == "__main__":
    main()
