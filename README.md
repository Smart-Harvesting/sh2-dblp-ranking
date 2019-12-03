# sh2-dblp-ranking

Code for ranking conferences of dblp by urgency of ingestion, and evaluating the result based on a gold standard.

Part of the DFG-funded research project ["Smart Harvesting II"](https://irgroup.github.io/smart-harvesting2/).

An earlier version of this code has been used to produce the following publication:

    @inproceedings{neumann2018prioritizing,
        title = {Prioritizing and Scheduling Conferences for Metadata Harvesting in dblp},
        author = {Neumann, Mandy and Michels, Christopher and Schaer, Philipp and Ralf, Schenkel},
        booktitle = {JCDL '18 Proceedings of the 18th ACM/IEEE on Joint Conference on Digital Libraries },
        doi = {10.1145/3197026.3197069},
        eventdate = {June 03 - 07, 2018},
        eventtitle = {18th ACM/IEEE on Joint Conference on Digital Libraries},
        venue = {Fort Worth, Texas, USA},
        isbn = {978-1-4503-5178-2},
        pages = {45-48},
        publisher = {ACM},
        address = {New York, NY, USA},
        url = {https://dl.acm.org/citation.cfm?doid=3197026.3197069},
        year = 2018
    }
    
 A preprint of this paper is available on [arXiv](https://arxiv.org/abs/1804.06169).
    
 ## Prerequisites
 
 The execution of this workflow relies on the existence of a database containing metadata related to the conferences like event dates, ingestion dates, publishing authors etc.
 This database is created from dblp data with the help of dblp-internal software that is not publicly available.
 
 To run the code, you would need to set up your own database, following a specific schema (see below).
 Then, create configuration files according to the provided [templates](src/main/resources/config) to specify database connection parameters.
 
 ### Database schema
 
 TODO
