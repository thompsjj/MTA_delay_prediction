# MTA_delay_prediction
Zipfian academy project 1

The prediction of behavior within a transportation network has been a topic of significant machine-learning research interest over the last 30 years. Although the study of such a system may appear to be mundane to the layman, it actually poses a unique and well-defined problem: a highly constrained network that responds as a whole to chaotic real-world perturbations such as mechanical failures, weather, traffic accidents, as well as human aspects, such as illness, crowding, late operators, and so on. These features make the train problem a plausible model case for the study of related, more complex problems such as networks of genes in cells. 
Beyond the aspect of research interest, the ability to provide a reasonable estimate of complex and changing conditions within transport systems such as delays can profoundly improve customer experience and provide value points of action where management can focus attention to improve efficiency and thereby expand their customer base.
Despite the research effort applied to this problem, robust models of train delay still elude researchers and the area remains a topic of modern research. This problem is compounded by the lack of clean, organized data germane to the problem, as transit systems are disinclined to make failure data available to the public.


This project focuses on developing the foundation for exploring solutions to this problem with the Bay Area Rapid Transit (BART) system. To do so, I propose the development and publication of two components.

First, I propose to produce a PostgreSQL database of BART delay times, for every train, for every station, for every day of the week. Approximately 95% of the code to extract this database has been written. It includes a set of handlers and objects and a daemon that actively scans the BART ETD API and compares this output to the published schedule. Several issues surrounding train times had to be overcome, particularly the presence of cancelled or rerouted trains, and numerous mismatches between the API labeling and the published BART scheduling. Sampling will be set up on an AWS server and collection run continuously for a period of time.

Second, I will produce a predictive model of delays once the database has been developed. Three approaches have been reasonably successful so far: neural networks1, graphical models2 and Markov chain methods3. In the first case, a number of libraries are available including FANN, neurolab, Theano and PyBrain. In the second, libpgm and PyMC are available as support for building a custom model. Validation will require some forward testing. The prediction results can be visualized in terms of colors (relative delay time) superimposed on a map of the BART stations.

As a first step to getting to the final product, I will analyze the MTA (NYC
metro) rollup files and calculate delay processes into a separate database to determine if a model can be established. This has the advantage of using a database for a system that is more or less completely sampled. 

Next steps to be taken:

1. Upload current code base for delay calculation.

2. Implement GTFS-realtime bindings for the MTA data.

3. Construct the equivalent postgreSQL database of delays with the current
codebase.

4. Implement a libpgm model for just one or two lines.

5. Visualize results of the test case.

