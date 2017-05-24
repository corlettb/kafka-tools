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
        parser.add_argument('-b', '--brokers', help="List of Broker IDs to remove", required=True, type=int, nargs='*')
        parser.add_argument('-tb', '--to_brokers', help="List of Broker IDs to move partitions to (defaults to whole cluster)",
                            required=False, type=int, nargs='*', default=[])

    #Very simple minded hash
    def hashval(self, str, siz):
        hash = 0
        # Take ordinal number of char in str, and just add
        for x in str: hash += (ord(x))
        return(hash % siz) # Depending on the range, do a modulo operation.

    def process_cluster(self):
        # TODO: Work in broker rack info to make sure replicas aren't assigned to the same racks

        for topic in self.cluster.topics:
            if self.args.topics != None and topic not in self.args.topics:
                continue
            if topic in self.args.exclude_topics:
                continue
            leader_deque = deque(self.to_brokers)
            todeque = deque(self.to_brokers)

            broker_start = self.hashval(topic, len(self.to_brokers))
            for _ in range(0, broker_start):
                proposed = leader_deque.popleft()
                leader_deque.append(proposed)
                proposed = todeque.popleft()
                todeque.append(proposed)
            proposed = todeque.popleft()
            todeque.append(proposed)

            sorted_partitions = sorted(self.cluster.topics[topic].partitions, key=lambda k: k.num)

            for partition in sorted_partitions:

                first = True

                leader_broker = None
                replica_count = len(partition.replicas)
                partition.remove_all_replicas()

                prepend_lender = None

                for pos in range(0, replica_count):
                    proposed = None
                    proposed_broker = None
                    if first:
                        proposed = leader_deque.popleft()
                        leader_deque.append(proposed)
                        proposed_broker = self.cluster.brokers[proposed]
                        leader_broker = proposed_broker
                    else:
                        proposed = todeque.popleft()
                        proposed_broker = self.cluster.brokers[proposed]
                        if proposed_broker == leader_broker:
                            prepend_lender = proposed
                            proposed = todeque.popleft()
                            proposed_broker = self.cluster.brokers[proposed]

                        todeque.append(proposed)

                    partition.add_replica(proposed_broker, pos)
                    first = False

                if prepend_lender != None:
                    todeque.appendleft(prepend_lender)
