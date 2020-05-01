# cinetflow
Conception and Implementation of a realtime asset database based on NetFlow data

### Background
NetFlow is a feature that was introduced on Cisco routers around 1996 that provides the ability to collect IP network traffic as it enters or exits an interface. A NetFlow contains various information, source and destination address, ports, protocols, timestamps and more. A NetFlow collector will gather flows from many different devices (routers, switches, etc.) and visualize communication between hosts on the network. Many commercial and open-source software products exist for this purpose.

The Leibniz Supercomputing Centre of the Bavarian Academy of Sciences and Humanities (LRZ) operates the Munich Scientific Network (MWN). The MWN consists of a backbone network with routers and switches for connecting the networks of the institutions at the various locations. Every communication on this network can be made visible using NetFlow.

While traditional NetFlow Collectors focus on calculating and visualizing many different scenarios (e.g. most used application on the network, top talkers, and many more), an asset database is rarely included even though the information can already be found in the data. Another problem is the amount of flows generated in a large-scale network such as the MWN. With more than 350,000 flows per second, processing the data in realtime is a challenge for most systems.

The outcome of this thesis should be a concept and prototypical implementation of a realtime asset database using the latest technology to process as many flows as possible in realtime and visualize active devices on the network.


# Todo:
1. Generate millions of IP addresses
2. Write the first chapter of the master thesis
