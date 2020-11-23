# structarray

We propose here a format to dump and decode any C structure, even if it is made itself of other structures or arrays (to be implemented)

The idea is:

* to record the structure (this was intended to be the scade context structure) each cycle, in a file, as this.
* to map the structure (each variable is associated to an address and a type)
* to decode the recorded file in order to convert it eventually in .tsv for analysis