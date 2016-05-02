Mode 1: Prepare Videos for Annoators

Create video clips and a JSON file to log the start and end time of a clip and annotation that correspond with each other. Start and end times for video clips are currently specified by initial Elan Annotations. TODO: create Elan files for each clip in prep for annotators.

Mode 2: Prepare Annotations and Data for Modelling

Take preexisiting JSON file with information about where video clip durations are with respect to a global timing reference, and look through eaf files for annotations. Then create a csv file (labels.csv) with each label in a column, writing 1 when the annotation exists, 0 when the annotation doesn't exist in a vido clip, and -1 when there isn't a video clip. Also extract out data corresponding to time segments in the videos clips with annotations (TODO.. partially complete)


TODO List:

1. Mode 1: (basically done - just need to auto-add tiers) auto setup eaf flies for annotators

2. Mode 1: Move master eaf file to somewhere else

3. Annotate

4. Mode 2: Check if eaf files are read properly according to JSON mappings

5. Mode 2: Dont Extract redundant data points for input in ROS