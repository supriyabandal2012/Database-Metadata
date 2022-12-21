import sys
from fileinput import filename

from Parser import Parser
from Executor import Executor


# Main entry of the application
# determines the queries and runs them
def main(argv):
    # Load the program
    # - variables
    # - make sure PA4 folder exists
    # - settings
    # - parser
    settings = {
        "database": None,
        "transaction": False,
        "cache": {}
    }
    parser = Parser()
    execute = Executor()

    file_detected = len(argv) > 1
    inputs = None
    while True:
        # build input.
        # Input could come from file or manual input in CLI.
        if file_detected:
            file_path = argv[1]
            try:
                with open(file_path, 'r') as file:
                    inputs = file.readlines()
            except FileNotFoundError:
                print("Invalid file path.")
            else:
                for inp in inputs:
                    if inp.strip().lower() == ".exit":
                        print("All done.")
                        return
        else:
            inputs = input()

            if inputs.lower() == ".exit":
                print("All done.")
                break

        # initial validation of input
        if parser.validate(inputs) is False:
            continue

        query = parser.query
        parser.clean()  # remove saved query in parser

        try:
            query = parser.parse(query)
            context = execute.create_context(query, settings)
            result = execute.run(context)
            if result:
                print(result.to_string())

            # Exit program when a file is used.
            if file_detected:
                break
        except:
            print("Invalid input.")


if __name__ == "__main__":
    main(sys.argv)
