import os
import time
class debug:
    def __init__(self,file_out=None):
        #print(file_out)
        self.file_out = file_out
        self.priority = 0
        self.console = True
    def log(self,*args,priority=0,sep=" ",end="\n"):
        self.Time_Stamp = True
        if priority >= self.priority:
            timestamp = ""
            if self.Time_Stamp:
                timestamp = self.getTimeString()
            if self.console:
                print(timestamp,*args,sep=sep,end=end)
            #print(self.file_out)
            if not self.file_out is None:
                f = open(self.file_out,"a")
                f.write(timestamp)
                first_sep = self.Time_Stamp
                for arg in args:
                    if first_sep:
                        f.write(sep)
                        first_sep = True
                    f.write(str(arg))
                f.write(end)
                f.close()
    def getTimeString(self,type= 'local'):
        type_dict = {'local':time.localtime,'GMT':time.gmtime}
        time_obj = type_dict[type]()
        month = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
        return "[ "+str(time_obj.tm_mday) +" "+ month[time_obj.tm_mon] + " " + str(time_obj.tm_year) + " " + str(time_obj.tm_hour)+":"+str(time_obj.tm_min)+":"+str(time_obj.tm_sec)+" ]"



if __name__ == "__main__":
    console = debug(out="test.log")
    console.log("This is a test")
