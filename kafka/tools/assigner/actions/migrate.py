# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from collections import deque
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.exceptions import ConfigurationException, NotEnoughReplicasException
import sys

class ActionMigrate(ActionModule):
    name = "migrate"
    helpstr = "Migrate topic partitions from one set of brokers to one or more other brokers (maintaining RF) and balancing the topics"

    def __init__(self, args, cluster):
        super(ActionMigrate, self).__init__(args, cluster)

        self.check_brokers(type_str="Brokers to remove")
        if len(set(self.args.to_brokers) & set(self.args.brokers)) != 0:
            raise ConfigurationException("Brokers to remove were specified in the target broker list as well")

        self.brokers = args.brokers
        self.to_brokers = args.to_brokers
        if len(self.to_brokers) == 0:
            self.to_brokers = list(set(self.cluster.brokers.keys()) - set(self.brokers))
        if set(self.args.to_brokers) & set(self.cluster.brokers.keys()) != set(self.args.to_brokers):
            raise ConfigurationException("Target broker are not in the brokers list for this cluster")

    @classmethod
    def _add_args(cls, parser):
        # should this be a main option?
        parser.add_argument('-t', '--topics', help="List of topics to include when performing actions", nargs='*')
        parser.add_argument('-b', '--brokers', help="List of Broker IDs to remove", type=int, nargs='*', default=[])
        parser.add_argument('-r', '--replicas', help="Force a certain number of replicas", type=int, default=-1)
        parser.add_argument('-tb', '--to_brokers', help="List of Broker IDs to move partitions to (defaults to whole cluster)",
                            required=False, type=int, nargs='*', default=[])

    #Very simple minded hash
    def hashval(self, str, siz):
        hash = 0
        # Take ordinal number of char in str, and just add
        for x in str: hash += (ord(x))
        return(hash % siz) # Depending on the range, do a modulo operation.

    def create_broker_deque(self, start_bias):

        my_deque = deque(self.to_brokers)

        for _ in range(0, start_bias):
            proposed = my_deque.popleft()
            my_deque.append(proposed)

        return my_deque

    def find_best_isr_broker(self, leader_broker, rack_count, broker_count, start_bias):

        brokers_deque = self.create_broker_deque(start_bias)

        best_broker = None
        lowest_tf_count = 99999999
        lowest_rack_count = 9999999

        while len(brokers_deque)> 0:
            proposed = brokers_deque.popleft()

            if proposed == leader_broker.id:
                # can't assign an isr to the leader
                continue

            if proposed not in broker_count:
                broker_count[proposed] = 0

            if self.cluster.brokers[proposed].rack not in rack_count:
                rack_count[self.cluster.brokers[proposed].rack] = 0

            if broker_count[proposed] < lowest_tf_count:
                best_broker = proposed
                lowest_tf_count = broker_count[proposed]

            if broker_count[proposed] == lowest_tf_count and rack_count[self.cluster.brokers[proposed].rack] < lowest_rack_count:
                best_broker = proposed
                lowest_tf_count = broker_count[proposed]

        broker_count[best_broker] = broker_count[best_broker] + 1
        rack_count[self.cluster.brokers[best_broker].rack] + 1
        return best_broker

    def process_cluster(self):


        for topic in self.cluster.topics:

            if self.args.topics != None and topic not in self.args.topics:
                continue
            if topic in self.args.exclude_topics:
                continue

            broker_start = self.hashval(topic, len(self.to_brokers))

            sorted_partitions = sorted(self.cluster.topics[topic].partitions, key=lambda k: k.num)

            broker_count = {}
            rack_count = {}
            leader_deque = self.create_broker_deque(broker_start)
            for partition in sorted_partitions:
                proposed = leader_deque.popleft()
                leader_deque.append(proposed)

                if self.cluster.brokers[proposed].rack in rack_count:
                    rack_count[self.cluster.brokers[proposed].rack] = rack_count[self.cluster.brokers[proposed].rack] +1
                else:
                    rack_count[self.cluster.brokers[proposed].rack] = 1

                if proposed in broker_count:
                    broker_count[proposed] = broker_count[proposed] + 1
                else:
                    broker_count[proposed] = 1

            leader_deque = self.create_broker_deque(broker_start)

            for partition in sorted_partitions:

                first = True

                leader_broker = None
                replica_count = None
                if self.args.replicas == -1:
                    replica_count = len(partition.replicas)
                else:
                    replica_count = self.args.replicas
                partition.remove_all_replicas()

                for pos in range(0, replica_count):
                    proposed = None
                    proposed_broker = None
                    if first:
                        proposed = leader_deque.popleft()
                        leader_deque.append(proposed)
                        proposed_broker = self.cluster.brokers[proposed]
                        leader_broker = proposed_broker
                    else:
                        best_broker = self.find_best_isr_broker(leader_broker, rack_count, broker_count, broker_start)
                        proposed_broker = self.cluster.brokers[best_broker]

                    partition.add_replica(proposed_broker, pos)
                    first = False

