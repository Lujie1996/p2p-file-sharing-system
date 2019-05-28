import sys


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
        if 'num_of_nodes_in_each_aws' not in configs:
            configs['num_of_nodes_in_each_aws'] = 5
        if 'port_start' not in configs:
            configs['port_start'] = 5001
        if 'heartbeat_timeout' not in configs:
            configs['heartbeat_timeout'] = 1
        if 'election_timeout_start' not in configs:
            configs['election_timeout_start'] = 3
        if 'election_timeout_end' not in configs:
            configs['election_timeout_end'] = 5
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


def load_matrix(matrix_path):
    matrix_list = []
    with open(matrix_path) as f:
        line = f.readline()
        while line:
            item = line.rstrip().split(" ")
            row = []
            for i in item:
                row.append(float(i))
            matrix_list.append(row)
            line = f.readline()
    return matrix_list


def init_start_server(local_ip):
    import os
    if os.path.exists("start_server.sh"):
       os.remove("start_server.sh")

    if os.path.exists("nodes_info.txt"):
       os.remove("nodes_info.txt")
    
    configs = load_config("config.txt")
    nodes = []
    start_server_commands = ["#!/bin/bash"]
    i = 0
    for ip, port in configs["nodes"]:
        nodes.append("node_index_{}, {}, {}".format(i, ip, port))
        if ip == local_ip:
           start_server_commands.append("nohup python3 server.py config.txt {} {} &".format(ip, port))
        i += 1
    with open("nodes_info.txt", 'w') as f:
        f.writelines("\n".join(nodes))

    with open("start_server.sh", 'w') as f:
        f.writelines("\n".join(start_server_commands))
    # generate matrix
    N = len(configs["nodes"])
    row = " ".join(["0.0" for i in range(N)])
    matrix = "\n".join([row for j in range(N)])
    with open("matrix", 'w') as f:
        f.writelines(matrix)

 
if __name__ == "__main__":
   if len(sys.argv) < 2:
     print("python3 utils.py $server_ip")
     sys.exit(1)
   ip = sys.argv[1]
   init_start_server(ip) 
