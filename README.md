# KbGs
GoldStandards for KnowladgeBases

This tool was crafted for comparing two Knowledgebases (RDF based) sharing the same unique Identifiers for same resources.

1. Initial steps include the preparation of the id files:
...
and the transformation of the Knowledgebase files into n-triples serialization.

2. Running the programm needs requires the generated id files of 1., the otriginal KB data files of both Knowledgebases.
Also the should be a mapping of same properties between both schemas of the KBs.

3. After finishing a resulting json file is produced, including the number of matching values compared to the 
number of resources with the same identifier.
