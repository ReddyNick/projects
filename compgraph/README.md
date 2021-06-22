# Computational graph library

This library provides an implementation of a MapReduce model for processing big data sets.
The model interface enables to build a graph-like description of the sequence of operations perfomed on data.   

## Getting started
Clone the library directory to your local machine.

### Installing
To use you need to install the library first.
    
    $ pip install compgraph

Then you can import compgraph in your environment.

## Running the tests
There are some tests in the directory that you can run.

Example of running all tests

    $ pytest compgraph


## Running the scripts
There are some examples of using compgraph library in /examples folder.
They use data from archive in /resources folder, so you need to extract it first.
Example

    python examples/run_word_count.py -i resources/text_corpus.txt -o <some folder>/output.txt


from YSDA python course with ❤️