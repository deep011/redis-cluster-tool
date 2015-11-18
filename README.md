# redis-cluster-tool

**redis-cluster-tool** is a convenient and useful tool for [redis cluster](https://github.com/antirez/redis). It was primarily built to manage the redis cluster.

## Build

redis-cluster-tool need hiredis-vip(https://github.com/vipshop/hiredis-vip), so you must install hiredis-vip first.

To build redis-cluster-tool:

    $ export HIREDIS_VIP_INSTALL_DIR="your hiredis-vip install path"
    $ export PREFIX="the path for redis-cluster-tool to install"
    $ make
    $ sudo make install

## Package

You can "yum install redis-cluster-tool" or "apt-get install redis-cluster-tool" instead of building from source code.

Before install the package, execute the follow command first:

**deb package** : `curl -s https://packagecloud.io/install/repositories/deep/packages/script.deb.sh | sudo bash`

**rpm package** : `curl -s https://packagecloud.io/install/repositories/deep/packages/script.rpm.sh | sudo bash`

You can also download the packages from "https://packagecloud.io/deep/packages" and install by yourself.

Attention : package redis-cluster-tool is depend on package hiredis-vip.

If you want to support other OS packages, please contact with me.
	
## Help

    Usage: redis-cluster-tool [-?hVds] [-v verbosity level] [-o output file]
                  [-c conf file] [-a addr] [-i interval]
                  [-p pid file] [-C command] [-r redis role]
                  [-t thread number] [-b buffer size]

    Options:
      -h, --help             : this help
      -V, --version          : show version and exit
      -d, --daemonize        : run as a daemon
      -s, --simple           : show the output not in detail
      -v, --verbosity=N      : set logging level (default: 5, min: 0, max: 11)
      -o, --output=S         : set logging file (default: stderr)
      -c, --conf-file=S      : set configuration file (default: conf/rct.yml)
      -a, --addr=S           : set redis cluster address (default: 127.0.0.1:6379)
      -i, --interval=N       : set interval in msec (default: 1000 msec)
      -p, --pid-file=S       : set pid file (default: off)
      -C, --command=S        : set command to execute (default: cluster_state)
      -r, --role=S           : set the role of the nodes that command to execute on (default: all, you can input: all, master or slave)
      -t, --thread=N         : set how many threads to run the job(default: 8)
      -b, --buffer=S         : set buffer size to run the job (default: 1048576 byte, unit:G/M/K)
    
    Commands:
        cluster_state                 :Show the cluster state.
        cluster_used_memory           :Show the cluster used memory.
        cluster_keys_num              :Show the cluster holds keys num.
        slots_state                   :Show the slots state.
        node_slot_num                 :Show the node hold slots number.
        new_nodes_name                :Show the new nodes name that not covered slots.
        cluster_rebalance             :Show the cluster how to rebalance.
        flushall                      :Flush all the cluster.
        cluster_config_get            :Get config from every node in the cluster and check consistency.
        cluster_config_set            :Set config to every node in the cluster.
        cluster_config_rewrite        :Rewrite every node config to echo node for the cluster.
        node_list                     :List the nodes
        del_keys                      :Delete keys in the cluster. The keys must match a given glob-style pattern.(This command not block the redis)
        
## Explanation

Now -d, -v, -o, -c, -i and -p options are not used, maybe they would be using in the future.

The command must be covered by double quotation marks, if there were more than one arguments.

## Example


**Get the cluster state:**

    $redis-cluster-tool -a 127.0.0.1:34501 -C cluster_state -r master
    master[127.0.0.1:34504] cluster_state is ok 
    master[127.0.0.1:34501] cluster_state is ok 
    master[127.0.0.1:34502] cluster_state is ok 
    master[127.0.0.1:34503] cluster_state is ok 
    all nodes cluster_state is ok

    
**Get the cluster used memory:**

    $redis-cluster-tool -a 127.0.0.1:34501 -C cluster_used_memory -r master
    master[127.0.0.1:34504] used 195 M	25%
    master[127.0.0.1:34501] used 195 M	25%
    master[127.0.0.1:34502] used 195 M	25%
    master[127.0.0.1:34503] used 195 M	25%
    cluster used 780 M
    

**Rebalance the cluster slots:**

    $redis-cluster-tool -a 127.0.0.1:34501 -C cluster_rebalance
    --from e1a4ba9922555bfc961f987213e3d4e6659c9316 --to 785862477453bc6b91765ffba0b5bc803052d70a --slots 2048
    --from 437c719f50dc9d0745032f3b280ce7ecc40792ac --to cb8299390ce53cefb2352db34976dd768708bf64 --slots 2048
    --from a497fc619d4f6c93bd4afb85f3f8a148a3f35adb --to a0cf6c1f12d295cd80f5811afab713cdc858ea30 --slots 2048
    --from 0bdef694d08cb3daab9aac518d3ad6f8035ec896 --to 471eaf98ff43ba9a0aadd9579f5af1609239c5b7 --slots 2048

Then you can use "redis-trib.rb reshard --yes --from e1a4ba9922555bfc961f987213e3d4e6659c9316 --to 785862477453bc6b91765ffba0b5bc803052d70a --slots 2048 127.0.0.1:34501" to rebalance the cluster slots 
    

**Flushall the cluster:**

    $redis-cluster-tool -a 127.0.0.1:34501 -C flushall -s
    Do you really want to execute the "flushall"?
    please input "yes" or "no" :
    
    yes
    OK


**Get a config from every node in cluster:**

    $redis-cluster-tool -a 127.0.0.1:34501 -C "cluster_config_get maxmemory" -r master
    master[127.0.0.1:34501] config maxmemory is 1048576000 (1000MB)
    master[127.0.0.1:34502] config maxmemory is 1048576000 (1000MB)
    master[127.0.0.1:34503] config maxmemory is 1048576000 (1000MB)
    master[127.0.0.1:34504] config maxmemory is 1048576000 (1000MB)

    All nodes config are Consistent
    cluster total maxmemory: 4194304000 (4000MB)
    

**Set a config from every node in cluster:**

    $redis-cluster-tool -a 127.0.0.1:34501 -C "cluster_config_set maxmemory 10000000" -s
    Do you really want to execute the "cluster_config_set"?
    please input "yes" or "no" :
    yes
    
    OK

**Delete keys in the cluster:**

    $redis-cluster-tool -a 127.0.0.1:34501 -C "del_keys 1*"
    Do you really want to execute the "del_keys"?
    please input "yes" or "no" :
    yes
    delete keys job is running...
    delete keys job finished, deleted: 999999 keys, used: 4 s
	
## License

Copyright 2012 Deep, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
