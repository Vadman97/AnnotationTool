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
			for row in reader:
				if None in row: #none is a key
					row["body_idxs"] = row[None]
					del row[None]
				# print repr(row)
				t = float(row['time'])
				if t > totalEndMSec:
					break

				counter = 0
				fail = False
				# for fps alignment, use fps as t value incrementer instead of single_body_idx file timings
				while t not in timeIDMap: #used to match up row times with master times
					# print repr(t)
					t -= 1
					if t < totalStartMSec:
						fail = True
						break
					counter += 1
				if fail:
					continue

				if counter != 0:
					pass
					#print counter

				if personID != -1:
					if timeIDMap[t] != personID:
						continue

				if fps:
					t += (1000 / fps) / fps * frameCount

				frameCount += 1
				if frameCount == fps:
					frameCount = 0

				rowTime = (t - startTime)

				if (rowTime < 0):
					startTime += (t - startTime)
					rosTime = 0

				row["time"] = rowTime
				row["absTime"] = t

				writer.writerow(row)
				# write the row to corresponding csv file with adjusted timestamp <-- or some common counter
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
	timeIDMap = dict()
	time = totalStartMSec
	with open(absPath + "logSingleKinectBodyIdxInfo_" + ts + ".csv", "rb") as csvfile:
		reader = csv.DictReader(csvfile, delimiter=',')

		for row in reader:
			if fps == False:
				time = int(row["time"])
			else:
				if row["time"] < time:
					continue

			if time < totalStartMSec:
				continue
			if time > totalEndMSec:
				break

			if row["body_idxs"] is None:
				continue
			if '|' not in row["body_idxs"]:
				continue

			bidx = int(row["body_idxs"].split('|')[1])
			timeIDMap.update({time: bidx})

			if fps:
				time += 1000 / fps

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
				t = row["time"]
				row.pop("time")
				labelsDict[t] = row
			csvFile.close()

	# print labelsDict

	labelData = parse_eaf.getActionList(aPathComplete)
	result = copy.deepcopy(labelsDict)
	result = collections.OrderedDict(sorted(result.items()))

	for time, row in labelsDict.iteritems():
		for action, instances in labelData.iteritems():
			if action in row:
				for inst in instances:
					start = inst[0]
					end = inst[1]
					if time >= start and time <= end:
						result[time][action] = 1
					else:
						result[time][action] = 0
				# del labelData[action]

	print result
	# TODO doesnt have the right 1/0 values for when the time is actually labeled

	with open(folderName + "/labels.csv", 'wb') as csvFile:
		print "WRITING"
		fnames = ["time"]
		for action in labelData:
			fnames.append(action)
		writer = csv.DictWriter(csvFile, delimiter=',', fieldnames=fnames)
		writer.writeheader()

		for time, row in result.iteritems():
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

    	labels = ["kicking", "hitting prep", "pointing", "shoving prep", "punching prep", "laughing", "teasing", "aggressive", "inappropriate", 
    	"single", "multiple", "showing fist", "awkward switch", "hitting", "punching", "false positive", "tongue?", "clip", "look over again"] #TODO AUTOMATICALLY GET THESE ALL
    	labelsDict = dict()
    	for time in timeIDMap:
    		labelsDict[time] = dict()
    		for label in labels:
    			labelsDict[time][label] = "N/A"

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

	processVideo(folderName, absPath, ts, startMin, startSec, endMin, endSec, fps, noVideo = nV)

	csvList = [
		"CLMHeads", "KinectBeamAudio", "KinectBodies", "KinectFaces", "KinectHDFacePointsCameraSpace",
		"KinectHDFaces", "KinectRawAudio", "MultipleKinectBodyIdxInfo", "SingleKinectBodyIdxInfo"
	]


	totalStartMSec = (startMin * 60 + startSec) * 1000
	totalEndMSec = (endMin * 60 + endSec) * 1000

	timeIDMap = createTimingDict(absPath, totalStartMSec, totalEndMSec, ts, fps)

	# processCSVs(csvList, startTime, folderName, absPath, ts, startMin, startSec, endMin, endSec, fps, totalStartMSec, totalEndMSec, timeIDMap)
	# TODO UNCOMMENT ^

	#folder multiplePeopleDeidentifiedImageData
	#processKinectHDFaces("DrawingPointsKinectHDFace", )
	#folder singlePersonDeidentifiedImageData
	dataList = ["DrawingPointsCLMEyeGaze", "DrawingPointsCLMEyes", "DrawingPointsCLMFace", "DrawingPointsKinectHDFace"]
	#processSinglePersonData(dataList)
	if annotations:
		processAnno(startTime, folderName, absPath, ts, startMin, startSec, endMin, endSec, totalStartMSec, totalEndMSec, timeIDMap)

	return "Success!"

def parseInputs(dataSet, expTime, startTime, endTime, fps = "", inFolder = "", annotations = False, nV = False):
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
	if (len(eaf) == 0):
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