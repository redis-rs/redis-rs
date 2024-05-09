import argparse
import json
import os
from os.path import join

"""Valkey command categorizer

This script analyzes command info json files and categorizes the commands based on their routing. The output can be used
to map commands in the cluster_routing.rs#base_routing function to their RouteBy category. Commands that cannot be 
categorized by the script will be listed under the "Uncategorized" section. These commands will need to be manually
categorized.

To use the script:
1. Clone https://github.com/valkey-io/valkey
2. cd into the cloned valkey repository and checkout the desired version of the code, eg 7.2.5
3. cd into the directory containing this script
4. run:
    python get_command_info.py --commands-dir=<path to cloned valkey repo>/valkey/src/commands
"""


class CommandCategory:
    def __init__(self, name, description):
        self.name = name
        self.description = description
        self.commands = []

    def add_command(self, command_name):
        self.commands.append(command_name)


def main():
    parser = argparse.ArgumentParser(
        description="Analyzes command info json and categorizes commands into their RouteBy categories")
    parser.add_argument(
        "--commands-dir",
        type=str,
        help="Path to the directory containing the command info json files (example: ../../valkey/src/commands)",
        required=True,
    )

    args = parser.parse_args()
    commands_dir = args.commands_dir
    if not os.path.exists(commands_dir):
        raise parser.error("The command info directory passed to the '--commands-dir' argument does not exist")

    all_nodes = CommandCategory("AllNodes", "Commands with an ALL_NODES request policy")
    all_primaries = CommandCategory("AllPrimaries", "Commands with an ALL_SHARDS request policy")
    multi_shard = CommandCategory("MultiShardNoValues or MultiShardWithValues",
                                  "Commands with a MULTI_SHARD request policy")
    first_arg = CommandCategory("FirstKey", "Commands with their first key argument at position 1")
    second_arg = CommandCategory("SecondArg", "Commands with their first key argument at position 2")
    second_arg_numkeys = (
        CommandCategory("SecondArgAfterKeyCount",
                        "Commands with their first key argument at position 2, after a numkeys argument"))
    # all commands with their first key argument at position 3 have a numkeys argument at position 2,
    # so there is a ThirdArgAfterKeyCount category but no ThirdArg category
    third_arg_numkeys = (
        CommandCategory("ThirdArgAfterKeyCount",
                        "Commands with their first key argument at position 3, after a numkeys argument"))
    streams_index = CommandCategory("StreamsIndex", "Commands that include a STREAMS token")
    second_arg_slot = CommandCategory("SecondArgSlot", "Commands with a slot argument at position 2")
    uncategorized = (
        CommandCategory(
            "Uncategorized",
            "Commands that don't fall into the other categories. These commands will have to be manually categorized."))

    categories = [all_nodes, all_primaries, multi_shard, first_arg, second_arg, second_arg_numkeys, third_arg_numkeys,
                  streams_index, second_arg_slot, uncategorized]

    print("Gathering command info...\n")

    for filename in os.listdir(commands_dir):
        file_path = join(commands_dir, filename)
        _, file_extension = os.path.splitext(file_path)
        if file_extension != ".json":
            print(f"Note: {filename} is not a json file and will thus be ignored")
            continue

        file = open(file_path)
        command_json = json.load(file)
        if len(command_json) == 0:
            raise Exception(
                f"The json for {filename} was empty. A json object with information about the command was expected.")

        command_name = next(iter(command_json))
        command_info = command_json[command_name]
        if "container" in command_info:
            # for two-word commands like 'XINFO GROUPS', the `next(iter(command_json))` statement above returns 'GROUPS'
            # and `command_info['container']` returns 'XINFO'
            command_name = f"{command_info['container']} {command_name}"

        if "command_tips" in command_info:
            request_policy = get_request_policy(command_info["command_tips"])
            if request_policy == "ALL_NODES":
                all_nodes.add_command(command_name)
                continue
            elif request_policy == "ALL_SHARDS":
                all_primaries.add_command(command_name)
                continue
            elif request_policy == "MULTI_SHARD":
                multi_shard.add_command(command_name)
                continue

        if "arguments" not in command_info:
            uncategorized.add_command(command_name)
            continue

        command_args = command_info["arguments"]
        split_name = command_name.split()
        if len(split_name) == 0:
            raise Exception(f"Encountered json with an empty command name in file '{filename}'")

        json_key_index, is_key_optional = get_first_key_info(command_args)
        # cluster_routing.rs can handle optional keys if a keycount of 0 is provided, otherwise the command should
        # fall under the "Uncategorized" section to indicate it will need to be manually inspected
        if is_key_optional and not is_after_numkeys(command_args, json_key_index):
            uncategorized.add_command(command_name)
            continue

        if json_key_index == -1:
            # the command does not have a key argument, check for a slot argument
            json_slot_index, is_slot_optional = get_first_slot_info(command_args)
            if is_slot_optional:
                uncategorized.add_command(command_name)
                continue

            # cluster_routing.rs considers each word in the command name to be an argument, but the json does not
            cluster_routing_slot_index = -1 if json_slot_index == -1 else len(split_name) + json_slot_index
            if cluster_routing_slot_index == 2:
                second_arg_slot.add_command(command_name)
                continue

            # the command does not have a slot argument, check for a "STREAMS" token
            if has_streams_token(command_args):
                streams_index.add_command(command_name)
                continue

            uncategorized.add_command(command_name)
            continue

        # cluster_routing.rs considers each word in the command name to be an argument, but the json does not
        cluster_routing_key_index = -1 if json_key_index == -1 else len(split_name) + json_key_index
        if cluster_routing_key_index == 1:
            first_arg.add_command(command_name)
            continue
        elif cluster_routing_key_index == 2:
            if is_after_numkeys(command_args, json_key_index):
                second_arg_numkeys.add_command(command_name)
                continue
            else:
                second_arg.add_command(command_name)
                continue
        # there aren't any commands that fall into a ThirdArg category,
        # but there are commands that fall under ThirdArgAfterKeyCount category
        elif cluster_routing_key_index == 3 and is_after_numkeys(command_args, json_key_index):
            third_arg_numkeys.add_command(command_name)
            continue

        uncategorized.add_command(command_name)

    print("\nNote: the following information considers each word in the command name to be an argument")
    print("For example, for 'XGROUP DESTROY key group':")
    print("'XGROUP' is arg0, 'DESTROY' is arg1, 'key' is arg2, and 'group' is arg3.\n")

    for category in categories:
        print_category(category)


def get_request_policy(command_tips):
    for command_tip in command_tips:
        if command_tip.startswith("REQUEST_POLICY:"):
            return command_tip[len("REQUEST_POLICY:"):]

    return None


def get_first_key_info(args_info_json) -> tuple[int, bool]:
    for i in range(len(args_info_json)):
        info = args_info_json[i]
        if info["type"].lower() == "key":
            is_optional = "optional" in info and info["optional"]
            return i, is_optional

    return -1, False


def get_first_slot_info(args_info_json) -> tuple[int, bool]:
    for i in range(len(args_info_json)):
        info = args_info_json[i]
        if info["name"].lower() == "slot":
            is_optional = "optional" in info and info["optional"]
            return i, is_optional

    return -1, False


def is_after_numkeys(args_info_json, json_index):
    return json_index > 0 and args_info_json[json_index - 1]["name"].lower() == "numkeys"


def has_streams_token(args_info_json):
    for arg_info in args_info_json:
        if "token" in arg_info and arg_info["token"].upper() == "STREAMS":
            return True

    return False


def print_category(category):
    print("============================")
    print(f"Category: {category.name} commands")
    print(f"Description: {category.description}")
    print("List of commands in this category:\n")

    if len(category.commands) == 0:
        print("(No commands found for this category)")
    else:
        category.commands.sort()
        for command_name in category.commands:
            print(f"{command_name}")

    print("\n")


if __name__ == "__main__":
    main()
