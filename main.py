import csv
import os
import shutil
import glob
import copy
import subprocess
import math
import parse_eaf
import json
import collections
import sys
#from multiprocessing import Process, Manager
from joblib import Parallel, delayed

ABS_PATH_TO_DATA = "../Data/"

def processVideo(folderName, absPath, ts, startMin, startSec, endMin, endSec, fps, noVideo = False):
	if noVideo:
		return
	totalStartSec = startMin * 60 + startSec
	totalEndSec = endMin * 60 + endSec
	frameStart = totalStartSec * 30;
	frameEnd = totalEndSec * 30;

	frames = list()

	for img in os.listdir(absPath + "testOutput_" + ts):
		if "frame" in img:
			imgFrame = int(img.split("frame")[1].split(".png")[0])
			if imgFrame >= frameStart and imgFrame <= frameEnd:
				frames.append(imgFrame)

	frames.sort()

	if not os.path.exists(folderName + '/frames'):
		os.makedirs(folderName + '/frames')

	idx = 1
	for frame in frames:
		shutil.copyfile(absPath + "testOutput_" + ts + '/frame' + str(frame) + '.png', folderName + '/frames/frame' + str(idx) + '.png')
		idx += 1

	#subprocess.call(["ffmpeg", "-framerate", "30", '-i', folderName+'/frames/frame%d.png', '-c:v', 'libx264', '-r', '30', '-pix_fmt', 'yuv420p', folderName+'/out.mp4'])
	proc = subprocess.Popen(["ffmpeg", "-framerate", "30", '-i', folderName+'/frames/frame%d.png', '-c:v', 'libx264', '-r', '30', '-pix_fmt', 'yuv420p', '-n', folderName+'/out.mp4'], shell=True, stdin=None, stdout=None, stderr=None, close_fds=True)


def writeRow(timeIDMap, curTime, previousRow, writer, personID):
	prevTime = float(previousRow['time'])
 	timeCounter = prevTime
 	while timeCounter < curTime:
 		#print "timeCounter " + repr(timeCounter) + " in range " + repr(prevTime) + " to " + repr(t)
 		if timeCounter in timeIDMap:
 			#print timeCounter
			if personID != -1:
 				if timeIDMap[timeCounter] == personID:
 					rowToWrite = previousRow
 					rowToWrite["absTime"] = timeCounter
 					writer.writerow(rowToWrite)

 			break
 		timeCounter += 1

def writeCSV(timeIDMap, inFile, outFile, totalStartMSec, totalEndMSec, startTime, personID = -1, fps = False):
	fi = open(inFile, 'rb')
	data = fi.read()
	fi.close()
	if '\x00' in data:
		fo = open(inFile, 'wb')
		fo.write(data.replace('\x00', '0'))
		fo.close()

	print inFile

	with open(inFile, 'rb') as csvfile:
		with open(outFile, 'wb') as outfile:
			reader = csv.DictReader(csvfile, delimiter=',')
			fieldnames = copy.deepcopy(reader.fieldnames)
			fieldnames.insert(0, 'absTime')
			writer = csv.DictWriter(outfile, delimiter=',', fieldnames=fieldnames)
			writer.writeheader()

			frameCount = 0
			previousRow = dict()
			rowToWrite = dict()
			firstRowToCheck = True

			for row in reader: 
				if None in row:
					row["body_idxs"] = row[None]
					del row[None]
				t = float(row['time'])
				if t < totalStartMSec:
					continue
				if t > totalEndMSec:
				 	break
				if (firstRowToCheck):
				 	firstRowToCheck = False
				else:
					writeRow(timeIDMap, t, previousRow, writer, personID)
				previousRow = row
			writeRow(timeIDMap, t, previousRow, writer, personID)
		outfile.close()
	csvfile.close()

# def processCSVHelper(csvFile, ts, timeIDMap, folderName, totalStartMSec, totalEndMSec, startTime, absPath):
# 	name = "log" + csvFile + "_" + ts + "_person"
# 	name2 = "log" + csvFile + "_" + ts
# 	for file in os.listdir(absPath):
# 		if name in file and ".csv" in file:
# 			personIdx = int(file.split(name)[1].split(".csv")[0])
# 			if personIdx not in timeIDMap.values():
# 				continue
# 			writeCSV(timeIDMap, absPath + file, folderName + '/' + file, totalStartMSec, totalEndMSec, startTime, personID = personIdx)	
# 		elif name2 in file:
# 			writeCSV(timeIDMap, absPath + file, folderName + '/' + file, totalStartMSec, totalEndMSec, startTime)
def processCSVHelper2(file, name, name2, timeIDMap, absPath, folderName, totalStartMSec, totalEndMSec, startTime, fps):
	if name in file and ".csv" in file:
		personIdx = int(file.split(name)[1].split(".csv")[0])
		if personIdx not in timeIDMap.values():
			return#continue
		writeCSV(timeIDMap, absPath + file, folderName + '/' + file, totalStartMSec, totalEndMSec, startTime, personID = personIdx, fps = fps)	
	elif name2 in file:
		writeCSV(timeIDMap, absPath + file, folderName + '/' + file, totalStartMSec, totalEndMSec, startTime, fps = fps)

def createTimingDict(absPath, totalStartMSec, totalEndMSec, ts, fps):
	timeIDMap = dict()#collections.OrderedDict()
	time = totalStartMSec
	setFirstFpsTime = True;
	previousRow = dict()
	with open(absPath + "logSingleKinectBodyIdxInfo_" + ts + ".csv", "rb") as csvfile:
		reader = csv.DictReader(csvfile, delimiter=',')
		for row in reader:
			time = int(row["time"])

			if time < totalStartMSec:
				continue
			if time > totalEndMSec:
				break

			#makes sure that bidx has proper value
			if (row["body_idxs"] is None) or ('|' not in row["body_idxs"]):
				bidx = -1
			else:
				bidx = int(row["body_idxs"].split('|')[1])

			if fps == False:
				timeIDMap.update({time: bidx})
			else:
				#if fps != False, set the timing in the map according to fpsTime
				if (setFirstFpsTime):
					fpsTime = time
					setFirstFpsTime = False;
				else:
					while(fpsTime < time):
						timeIDMap.update({fpsTime: previousRow["body_idx"]})
						fpsTime += 1000/fps

			previousRow["body_idx"] = bidx
			previousRow["time"] = time
		if fps:
			timeIDMap.update({fpsTime: previousRow["body_idx"]})
	return timeIDMap

def processCSVs(csvList, startTime, folderName, absPath, ts, startMin, startSec, endMin, endSec, fps, totalStartMSec, totalEndMSec, timeIDMap):
	for csvFile in csvList:
		name = "log" + csvFile + "_" + ts + "_person"
		name2 = "log" + csvFile + "_" + ts
		Parallel(n_jobs = 8)(delayed(processCSVHelper2)(file, name, name2, timeIDMap, absPath, folderName, totalStartMSec, totalEndMSec, startTime, fps) for file in os.listdir(absPath))

	#Parallel(n_jobs = 5)(delayed(processCSVHelper)(csvFile, ts, timeIDMap, folderName, totalStartMSec, totalEndMSec, startTime, absPath) for csvFile in csvList)
	# print repr(timeIDMap)
	# for time, bIdx in timeIDMap.iteritems():
	# Parallel(n_jobs = 5) (delayed(for csvFile in csvList))

	# for csvFile in csvList:
	# 	name = "log" + csvFile + "_" + ts + "_person"
	# 	name2 = "log" + csvFile + "_" + ts
	# 	for file in os.listdir(absPath):
	# 		if name in file and ".csv" in file:
	# 			personIdx = int(file.split(name)[1].split(".csv")[0])
	# 			if personIdx not in timeIDMap.values():
	# 				print "person idx not in timeIDMap " + name
	# 				continue
	# 			writeCSV(timeIDMap, absPath + file, folderName + '/' + file, totalStartMSec, totalEndMSec, startTime, personID = personIdx)
				
	# 		elif name2 in file:
	# 			writeCSV(timeIDMap, absPath + file, folderName + '/' + file, totalStartMSec, totalEndMSec, startTime)				
	# 			#Simple file, just copy the range we want of the whole file no body idx mapping needed

def createAnnotation(startTime, folderName, absPath, aPathComplete, sTime, eTime, labelsDict):
	if (os.path.isfile(folderName + "/labels.csv")):
		with open(folderName + "/labels.csv", 'rb') as csvFile:
			reader = csv.DictReader(csvFile, delimiter=',')
			for row in reader:
				t = int(row["time"])
				row.pop("time")
				labelsDict[t] = row
			csvFile.close()
		os.remove(folderName + "/labels.csv")

	labelData = parse_eaf.getActionList(aPathComplete)
	result = copy.deepcopy(labelsDict)

	for time in sorted(labelsDict):
		row = labelsDict[time]
		for action, instances in labelData.iteritems():
			if action in row:
				for inst in instances:
					start = int(inst[0])
					end = int(inst[1])
					if int(time) >= start and time <= end:
						result[time][action] = 1
					elif int(result[time][action]) != 1:
						result[time][action] = 0

	with open(folderName + "/labels.csv", 'wb') as csvFile:
		print "WRITING"
		fnames = ["time"]
		for action in labelData:
			fnames.append(action)
		writer = csv.DictWriter(csvFile, delimiter=',', fieldnames=fnames)
		writer.writeheader()

		for time in sorted(result):
			row = result[time]
			r = dict()
			r["time"] = time
			r.update(row)
			writer.writerow(r)

		csvFile.close()

def processAnno(startTime, folderName, absPath, ts, startMin, startSec, endMin, endSec, totalStartMSec, totalEndMSec, timeIDMap):
	aPath = absPath + "annotation/"
	with open(aPath + 'map.json') as jFile:
		jsonData = json.load(jFile)
		# JSON format --> name of file: [start time ms, end time ms]
    	# print jsonData

    	labels = parse_eaf.getTierNames(aPath + "1.eaf") #ASSUMING THAT FIRST EAF FILE HAS ALL OF THE LABELS
    	print repr(labels)
    	#["kicking", "hitting prep", "pointing", "shoving prep", "punching prep", "laughing", "teasing", "aggressive", "inappropriate", 
    	#"single", "multiple", "showing fist", "awkward switch", "hitting", "punching", "false positive", "tongue?", "clip", "look over again", "backhand hitting"] #TODO AUTOMATICALLY GET THESE ALL
    	labelsDict = dict()
    	for time in timeIDMap:
    		labelsDict[time] = dict()
    		for label in labels:
    			labelsDict[time][label] = -1
    		
    	for file in os.listdir(aPath):
    		# print file
			if ".eaf" in file:
				f = file[:-4]
				if f in jsonData:
					sTime = jsonData[f][0]
					eTime = jsonData[f][1]
					createAnnotation(startTime, folderName, absPath, aPath + file, sTime, eTime, labelsDict)

def process(folderName, absPath, ts, startMin, startSec, endMin, endSec, fps, annotations, nV):
	startTime = 0

	#cleans up the data in logSingleKinectBodyIdxInfo_x.csv to be interpretable
	fi = open(absPath + "logSingleKinectBodyIdxInfo_" + ts + ".csv", 'rb')
	data = fi.read()
	fi.close()
	if '\x00' in data:
		fo = open(absPath + "logSingleKinectBodyIdxInfo_" + ts + ".csv", 'wb')
		fo.write(data.replace('\x00', '0'))
		fo.close()

	with open(absPath + "logSingleKinectBodyIdxInfo_" + ts + ".csv", "rb") as csvfile:
		reader = csv.DictReader(csvfile, delimiter=',')
		#for row in reader:
		row1 = reader.next()
		startTime = int(row1["time"])
		#print str(startTime)

	# process the video (copy frames and generate video), only when nV is set to False
	processVideo(folderName, absPath, ts, startMin, startSec, endMin, endSec, fps, noVideo = nV)

	csvList = [
		"CLMHeads", "KinectBeamAudio", "KinectBodies", "KinectFaces", "KinectHDFacePointsCameraSpace",
		"KinectHDFaces", "KinectRawAudio", "MultipleKinectBodyIdxInfo", "SingleKinectBodyIdxInfo"
	]


	totalStartMSec = (startMin * 60 + startSec) * 1000
	totalEndMSec = (endMin * 60 + endSec) * 1000
	#convert timing to MS

	timeIDMap = createTimingDict(absPath, totalStartMSec, totalEndMSec, ts, fps) #generate dictionary of timings that all csvs will follow (also has current body indx for any time)
	# print repr(sorted(timeIDMap))

	processCSVs(csvList, startTime, folderName, absPath, ts, startMin, startSec, endMin, endSec, fps, totalStartMSec, totalEndMSec, timeIDMap)
	#for each csv, process the data and output it based on the timeIDMap

	#folder multiplePeopleDeidentifiedImageData
	#processKinectHDFaces("DrawingPointsKinectHDFace", )
	#folder singlePersonDeidentifiedImageData
	dataList = ["DrawingPointsCLMEyeGaze", "DrawingPointsCLMEyes", "DrawingPointsCLMFace", "DrawingPointsKinectHDFace"] # TODO do we process this or is it redundant?
	#processSinglePersonData(dataList)
	if annotations:
		processAnno(startTime, folderName, absPath, ts, startMin, startSec, endMin, endSec, totalStartMSec, totalEndMSec, timeIDMap)
		#process the annotations to create our labels.csv file
		#uses annotations folder in data directory for annotations (map.json stores .eaf file names mapping to the timing the file is in the entire data set)

	return "Success!"

def parseInputs(dataSet, expTime, startTime, endTime, fps = "", inFolder = "", annotations = False, nV = False):
	#currently defaults to looking in RoboticsOpenHouse2016Data folder
	if len(dataSet) == 0:
		dataSet = "RoboticsOpenHouse2016Data"
	if len(expTime) == 0:
		expTime = "DataCollection_4-14-2016_9-5-46"
	else:
		expTime = "DataCollection_" + expTime

	if len(startTime) == 0:
		startTime = "0:0"
	if len(endTime) == 0:
		endTime ="1:0"
	if len(fps) == 0:
		fps = False
	else:
		fps = int(fps)

	ts = expTime.split('DataCollection_')[1]

	if len(startTime.split(':')) == 1:
		sec = float(startTime) / 1000.
		startSec = math.floor(sec) % 60
		startMin = math.floor(sec / 60.)
	else:
		startMin = startTime.split(':')[0]
		startSec = startTime.split(':')[1]

	if len(endTime.split(':')) == 1:
		esec = float(endTime) / 1000.
		endSec = math.floor(esec) % 60
		endMin = math.floor(esec / 60.)
	else:
		endMin = endTime.split(':')[0]
		endSec = endTime.split(':')[1]

	startMin = int(startMin)
	startSec = int(startSec)
	endMin = int(endMin)
	endSec = int(endSec)

	if startMin == endMin:
		if startSec == endSec:
			endSec += 1

	#ABS_PATH_TO_DATA is manually defined
	absPath = ABS_PATH_TO_DATA + dataSet + '/' + expTime + '/'

	if os.path.exists(ABS_PATH_TO_DATA + dataSet):
		if os.path.exists(ABS_PATH_TO_DATA + dataSet + '/' + expTime):

			if len(inFolder) != 0:
				prefix = str(inFolder) + "/"
			else:
				prefix = ""

			folderName = ABS_PATH_TO_DATA + prefix + "experiment_clip_" + dataSet + "_" + ts + "_" + str(startMin) + "." + str(startSec) + "-" + str(endMin) + "." + str(endSec)

			print "Output will be generated in folder: " + folderName

			if not os.path.exists(folderName):
				os.makedirs(folderName)
			return process(folderName, absPath, ts, startMin, startSec, endMin, endSec, fps, annotations, nV)
			#else:
			#	return "Error! Folder already exists for this clip"

	return "Error! Experiment with given data set and timestamp not found: " + repr(dataSet)


if __name__ == "__main__":
	dataSet = raw_input("Enter a data set folder name: ")
	expTime = raw_input("Enter an experiment timestamp (eg: 4-14-2016_14-24-38): ")
	fps = raw_input("Enter output video fps: ")
	eaf = raw_input("Enter a .eaf file for generation or press ENTER for manual entry: ")

	#will auto-parse an eaf file if it is provided.
	if (len(eaf) == 0): #no file
		startTime = raw_input("Enter the data start time (min:sec OR miliseconds): ") # (time from the processed video, eg: 10:30) or in miliseconds:
		endTime = raw_input("Enter the data end time (min:sec OR miliseconds): ") # (time from the processed video, eg: 10:35) or in miliseconds:
		processAnnotations = raw_input("Process annotations? (t,f): ")
		anno = True if processAnnotations == "t" or processAnnotations == "T" else False
		print str(parseInputs(dataSet, expTime, startTime, endTime, fps = fps, annotations = anno, nV = True))
	else:
		res = parse_eaf.getActionList(eaf)
		for action, instances in res.iteritems():
			a = str(action)
			a = a.replace(' ', '_')
			a = a.replace('?', '_maybe')
			for timeTuple in instances:
				startTime = str(timeTuple[0])
				endTime = str(timeTuple[1])
				print "S: " + startTime + " E: " + endTime
				print str(parseInputs(dataSet, expTime, startTime, endTime, inFolder=a, fps = fps))