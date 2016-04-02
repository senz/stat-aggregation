Parallel and distributed statistical calculation with anonymization for a JSON formatted file dataset.

# Instructions for compiling
SBT is 0.13 is required for compilation.

sbt-assembly plugin dep is included so:

`sbt assembly` will run tests and then generate PROJECT_ROOT/target/scala-2.11/stat-aggregator-assembly-PROJECT_VERSION.jar 
with all the required dependencies.

# Instructions for running

## Assembled jar
First argument of program must be a path to data directory, i.e.:

`java -jar ~/backend-problem/backend-problem-assembly-1.0.jar /home/user/data`

Path to data dir **must be an absolute path**, so no ~ or .. or other special shell symbols.

## Configurable options
Application uses typesafe config library, so config can be passed with -Dconfig.file=PATH_TO_CFG argument
 
### Format explanation
application {
  filterType = drink // for what type property calculations are 
  anonThreshold = 0 // anonymization theshold
  firstUserId = 0 // id of user to start parsing from
  lastUserId = 15  // last id of user to read
}

# Output result and explanations
`… result: min: 150, max: 10000, avg: 6924, median: 10000`

Those results are very accurate, for the small data set.

For median calculation I've chosen an approximation with technique called Reservoir sampling. It is using a fixed sized 
data structure (100000 elements by default) and uniformly random function, that will seed array if it is overloaded. 
Overloaded elements will be distributed equally across array. 

At the end of aggregated data collection, sorting performed only once before calculating result.

# Some features
* min, max, median, average calculations
* anonymization: user identity is not accessable during aggregation, and type positions with count below threshold are
removed
* parser actor can be scaled with router instance and potentially clustered (but problem with data distribution must be
solved first)
* user aggregator actor can be scaled also with little modification, supervising actor will route and assemble results
from user aggregator children to god actor
* global aggregator can be scaled and distributed with addition of distributed memory storage (like cassandra or memcached)
* whole system can be made more resilient with persistent message storage (activemq i.e.)


# Data format
```json
{
  "purchases": [
    {"type": "snack", "amount": 100},
    {"type": "restaurant", "amount": 25},
    {"type": "coffee", "amount": 2},
    {"type": "gas", "amount": 250}
  ]
}
```
