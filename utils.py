def load_config(config_path):
    configs = {}
    with open(config_path) as f:
        line = f.readline()
        while line:
            item = line.rstrip().split(": ")
            configs[item[0]] = eval(item[1])
            line = f.readline()
        if not configs:
            print("Lack of configurations")
            exit(1)

        # set default configs
        if 'servers' not in configs or len(configs['servers']) == 0:
            # server default to be localhost
            configs['servers'] = ['localhost']
        if 'port_start' not in configs:
            configs['port_start'] = 5001
        if 'rpc_timeout' not in configs:
            configs['rpc_timeout'] = 1

        # set node list
        configs["nodes"] = []
        # nodes = []
        # start_server_commands = ["#!/bin/bash"]
        for s_index, server_node in enumerate(configs['servers']):
            for i in range(configs['num_of_nodes_in_each_aws']):
                node_index = s_index * configs['num_of_nodes_in_each_aws'] + i
                node_port = str(configs['port_start'] + i)
                configs["nodes"].append((server_node, node_port))
                # nodes.append("node_index_{}, {}, {}".format(node_index, server_node, node_port))
                # start_server_commands.append("python3 server.py config.txt {} {} &".format(server_node, node_port))
    return configs