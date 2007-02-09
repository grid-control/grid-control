import os, string

from grid_control import Config

class UserMod:
    def __init__(self, config):
        self.config = config  
    def BuildJDL(self):
        exe = self.config.get('USER','executable')
        infiles = self.config.get('USER','inputfiles')
        outfiles = self.config.get('USER','outputfiles')
        requirements = self.config.get('grid','requirements')
        virtorg = self.config.get('grid','vo')
        file=open('userjob.jdl','w')
        file.write('Executable = "'+exe+'";\n')
        file.write('StdOutput = "std.out";\n')
        file.write('StdError = "std.err";\n')
        infilestr='InputSandbox = {"'+string.join(infiles.split(' '),'","')+'"};\n'
        file.write(infilestr)
        file.write('OutputSandbox = {"std.out","std.err","'+string.join(outfiles.split(' '),'","')+'"};\n')
        file.write('VirtualOrganisation = "'+virtorg+'";\n')
        file.write('Requirements = '+requirements+'\n')
    def SubmitJob(self):
        os.system("glite-job-submit -o id.txt userjob.jdl")
