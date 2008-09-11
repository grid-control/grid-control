from grid_control import Job, RuntimeError, utils, enumerate

class Report:

	def __init__(self, jobs, allJobs):
		self.allJobs = allJobs
		if hasattr(jobs, "all"):
			self.jobs = jobs.all
		else:
			self.jobs = jobs
	
	def details(self):
                reports = {}
                maxWidth = [6, 20, 0]

                # Get the maximum width of each column
                for id in self.jobs:
                        job = self.allJobs.get(id)
                        thisreport = job.report()
                        reports[id] = thisreport
                        if maxWidth[0] < len(thisreport[0]):
                                maxWidth[0] = len(thisreport[0])
                        if maxWidth[1] < len(thisreport[1]):
                                maxWidth[1] = len(thisreport[1])
                        if maxWidth[2] < len(thisreport[2]):
                                maxWidth[2] = len(thisreport[2])

                # Print table header
                print "\n%4s %-*s %s" % ("Job", maxWidth[0], "Status", "Destination / Job ID")

                # Calculate width of horizontal lines
                if maxWidth[1] > maxWidth[2]:
                        lineWidth = maxWidth[0]+maxWidth[1]+8
                else:
                        lineWidth = maxWidth[0]+maxWidth[2]+6
                line = "-"*lineWidth
                print line

                # Calculate spacer width for second row
                spacer = " "*(maxWidth[0]+5)

                # Get information of each job


		if not len(self.jobs):
			print " No jobs found!"
                for id in self.jobs:
                        print "%4d %-*s %s /" % (id, maxWidth[0], reports[id][0], reports[id][1])
                        print "%s %-*s" % (spacer, maxWidth[0]+maxWidth[2]+6, reports[id][2])

                print line + "\n"


	def siteReport(self):
		print "-----------------------------------------------------------------"

	def summary(self):
		# Print report summary
		print "-----------------------------------------------------------------"
		print "REPORT SUMMARY:"
		print "---------------"

		summary = map(lambda x: 0.0, Job.states)

		for id in self.allJobs.all:
			job = self.allJobs.get(id)
			summary[job.state] += 1

		def makeSum(*states):
			return reduce(lambda x, y: x + y, map(lambda x: summary[x], states))

		print "Total number of jobs:      %4d   Number of successful jobs: %4d" % \
			(len(self.allJobs.all), summary[Job.SUCCESS])
		print "Number of unfinished jobs: %4d   Number of failed jobs:     %4d\n" % \
			(makeSum(Job.INIT, Job.READY, Job.WAITING, Job.QUEUED, Job.SUBMITTED, Job.RUNNING),
			 makeSum(Job.ABORTED, Job.CANCELLED, Job.FAILED))
		print "Detailed Information:"
		for state, category in enumerate(Job.states):
			if summary[state]:
				ratio = (summary[state] / (id + 1))*100
			else:
				ratio = 0
			print "Jobs   %9s: %4d     %3d%%" % (category, summary[state], round(ratio))

		print "-----------------------------------------------------------------\n"
		return 0
