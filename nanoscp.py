#! python
import os, sys, glob, time
import threading, queue
import tarfile
import paramiko, socket
import re
import wx
import wx.lib
from wx.lib import intctrl
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
from watchdog.events import FileMovedEvent


# Handle file system events matching regex
class FileHandler(RegexMatchingEventHandler):
    def __init__(self, regex=['.*'], notify=None):
        super().__init__(regexes=regex, ignore_directories=True)
        self.notify = notify
        
    def on_any_event(self, event):
        if not event.is_directory and self.notify and not isinstance(event, FileMovedEvent):
            self.notify(event.src_path)


# Set with timestamp to retrieve items at least n seconds not touched
class TimedSet():
    def __init__(self):
        self.__data = dict()
        self.__condition = threading.Condition()

    def put(self, item):
        with self.__condition:
            self.__data[item] = time.time()

    def get(self, t_wait=0):
        items = []
        t_now = time.time()
        with self.__condition:
            keys = list(self.__data.keys())
            for key in keys:
                if self.__data[key] + t_wait <= t_now:
                    del self.__data[key]
                    items.append(key)
        return items


# Logger
class Log(list):
    def __init__(self, **kwargs):
        self.callbacks = []
        return super().__init__(**kwargs)
        
    def add_callback(self, callback):
        self.callbacks.append(callback)

    def append(self, object):
        for callback in self.callbacks:
            callback(object)
        return super().append(object)


# Archive sets of files as tar balls
class FileArchiver():
    def __init__(self, dst_path, name_prefix='', batch_size=4000, count_offset=0, log=Log()):
        self.dst_path = dst_path
        self.name_prefix = name_prefix
        self.batch_size = batch_size
        self.__current_count = count_offset
        self.__data_queue = []
        self.__archive_queue = []
        self.__condition = threading.Condition()
        self.__archive_worker = None
        self.log = log

    def __del__(self):
        if self.__archive_worker:
            self.stop()

    def add(self, file_name):
        with self.__condition:
            self.__data_queue.append(file_name)
            if len(self.__data_queue) >= self.batch_size:
                self.__archive_queue.append(self.__data_queue[:self.batch_size])
                del self.__data_queue[:self.batch_size]
                self.__condition.notify_all()

    def start(self):
        if not self.__archive_worker:
            self.__archive_worker = threading.Thread(target=self.__archiver__)
            self.__archive_worker.start()

    def stop(self, partial_write=True):
        if self.__archive_worker:
            with self.__condition:
                if partial_write and len(self.__data_queue) > 0:
                    self.__archive_queue.append(self.__data_queue)
                self.__archive_queue.append([])
                self.__condition.notify_all()
            self.__archive_worker.join()
            self.__archive_worker = None

    def __archiver__(self):
        while True:
            while len(self.__archive_queue) > 0:
                with self.__condition:
                    batches = self.__archive_queue.copy()
                    self.__archive_queue.clear()
                for batch in batches:
                    if not batch:
                        return  # poison pill exit
                    try:
                        dst = os.path.join(self.dst_path, self.name_prefix + str(self.__current_count) + '.tar')
                        if os.path.isfile(dst):
                            self.log.append('[ERROR] File ' + dst + ' already exists, skiped writing')
                            continue
                        with tarfile.open(dst, 'w') as fp:
                            for f in batch:
                                fp.add(f, arcname=os.path.basename(f))
                            self.log.append('Archived ' + str(len(batch)) + ' files as ' + self.name_prefix + str(self.__current_count) + '.tar')
                            self.__current_count += 1
                    except FileExistsError as e:
                        self.log.append('[ERROR] File ' + self.name_prefix + str(self.__current_count) + '.tar' + ' already exists, output NOT written')                
            with self.__condition:
                self.__condition.wait()


# SCP client
class SCP():
    def __init__(self):
        pass


# MinION Export Deamon app
class app_core():
    def __init__(self, log=Log()):
        self.source_path = ''
        self.export_path = ''
        self.batch_prefix = ''
        # run
        self.cell_id = 'FXX00000'
        self.cell_type = 'FLO-MIN10X'
        self.kit = 'None'
        self.usr1 = ''
        self.usr2 = ''
        # ssh

        # options
        self.regex = '.*fast5$'
        self.batch_size = 4000
        self.batch_offset = 0
        self.delay = 60
        self.recursive = False
        self.ignore_existing = False
        self.watchdog = None
        self.file_queue = None
        self.archiver = None
        self.log = log
        
    def is_startable(self):
        startable = True
        try:
            if not os.path.isdir(self.source_path):
                startable = False
                self.log.append('[ERROR] Source is not a directory')
            if not os.path.isdir(self.export_path):
                startable = False
                self.log.append('[ERROR] Destination is not a directory')
            if self.batch_size < 1:
                startable = False
                self.log.append('[ERROR] Batch size must be greater one')
        except:
            return False
        return startable

    def start_watchdog(self):
        try:
            self.archiver = FileArchiver(self.export_path, name_prefix=self.batch_prefix, 
                                         batch_size=self.batch_size, count_offset=self.batch_offset,
                                         log=self.log)
            self.archiver.start()
            self.file_queue = TimedSet()
            self.watchdog = FileHandler(regex=[self.regex], notify=self.on_file_event)
            self.observer = Observer()
            self.observer.schedule(self.watchdog, path=self.source_path, recursive=self.recursive)
            self.observer.start()
            self.log.append('[INFO] Started File System Observer')
            if not self.ignore_existing:
                if self.recursive:
                    existing = [os.path.join(dirpath, f) for dirpath, _, files in os.walk(self.source_path) for f in files if re.match(self.regex, f)]
                else:
                    existing = [os.path.join(self.source_path, f) for f in os.listdir(self.source_path) if re.match(self.regex, f)]
                for ex in existing:
                    self.archiver.add(ex)
                self.log.append('[INFO] Included ' + str(len(existing)) + ' existing files')
            return True
        except Exception as e:
            self.log.append('[ERROR] Starting watchdog failed')
            return False

    def stop_watchdog(self):
        try:
            self.observer.stop()
            self.observer.join()
            for name in self.file_queue.get(t_wait=0):
                if os.path.isfile(name):
                    self.archiver.add(name)
            self.archiver.stop()
            self.log.append('[INFO] Stopped File System Observer')
            return True
        except Exception as e:
            self.log.append('[ERROR] Stoping Watchdog failed')
            return False

    def on_file_event(self, file_name):
        self.file_queue.put(file_name)
        for name in self.file_queue.get(t_wait=self.delay):
            if os.path.isfile(name):
                self.archiver.add(name)


# Main Window
class app_window(wx.Frame):
    def __init__(self, parent, title):
        # ensure the parent's __init__ is called
        super(app_window, self).__init__(parent, title=title, size=(520,560), style=wx.CAPTION | wx.MINIMIZE_BOX | 
                                         wx.CLOSE_BOX | wx.RESIZE_BORDER | wx.CLIP_CHILDREN)
        # init app class
        self.log = Log()
        self.log.add_callback(self.on_log)
        self.app = app_core(log=self.log)
        # create a menu bar
        self.makeMenuBar()
        self.initUI()
        self.initEvents()
        self.Show()

    def makeMenuBar(self):
        # Make a file menu with Hello and Exit items
        fileMenu = wx.Menu()
        # When using a stock ID we don't need to specify the menu item's label
        exitItem = fileMenu.Append(wx.ID_EXIT)
        # Now a help menu for the about item
        helpMenu = wx.Menu()
        aboutItem = helpMenu.Append(wx.ID_ABOUT)
        # Main menu bar
        menuBar = wx.MenuBar()
        menuBar.Append(fileMenu, "&File")
        menuBar.Append(helpMenu, "&Help")
        # Give the menu bar to the frame
        self.SetMenuBar(menuBar)
        # Bind event handler
        self.Bind(wx.EVT_MENU, self.on_exit,  exitItem)
        self.Bind(wx.EVT_MENU, self.on_about, aboutItem)

    def initUI(self):
        panel = wx.Panel(self)
        sizer = wx.GridBagSizer(6, 5)
        # Destination settings
        dst_box = wx.StaticBox(panel, label="File System")
        dst_sizer = wx.StaticBoxSizer(dst_box, wx.VERTICAL)
        dst_grid = wx.GridBagSizer(3, 5)
        lbl_Source = wx.StaticText(panel, label="Source")      
        self.txt_Source = wx.TextCtrl(panel)
        self.btn_Source = wx.Button(panel, label="Browse...", size=(-1, 20), )
        lbl_Destination = wx.StaticText(panel, label="Local/ Temp")
        self.txt_Destination = wx.TextCtrl(panel)    
        self.btn_Destination = wx.Button(panel, label="Browse...", size=(-1, 20))
        lbl_ssh_key = wx.StaticText(panel, label="SCP Key")
        self.txt_ssh_key = wx.TextCtrl(panel)    
        self.btn_ssh_key = wx.Button(panel, label="Browse...", size=(-1, 20))
        lbl_ssh_host = wx.StaticText(panel, label="SCP Host")
        self.txt_ssh_host = wx.TextCtrl(panel)
        lbl_username = wx.StaticText(panel, label="SCP User")
        self.txt_ssh_user = wx.TextCtrl(panel)
        lbl_password = wx.StaticText(panel, label="SCP Password")
        self.txt_ssh_pw = wx.TextCtrl(panel, style=wx.TE_PASSWORD)
        dst_grid.Add(lbl_Source, pos=(0,0), flag=wx.LEFT|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(self.txt_Source, pos=(0,1), span=(1, 3), flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(self.btn_Source, pos=(0,4), flag=wx.RIGHT|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(lbl_Destination, pos=(1,0), flag=wx.LEFT|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(self.txt_Destination, pos=(1,1), span=(1, 3), flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(self.btn_Destination, pos=(1,4), flag=wx.RIGHT|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(lbl_ssh_host, pos=(2,0), flag=wx.LEFT|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(self.txt_ssh_host, pos=(2,1), span=(1, 1), flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(lbl_ssh_key, pos=(2,2), flag=wx.LEFT|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(self.txt_ssh_key, pos=(2,3), span=(1, 1), flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(self.btn_ssh_key, pos=(2,4), flag=wx.RIGHT|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(lbl_username, pos=(3,0), flag=wx.LEFT|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(self.txt_ssh_user, pos=(3,1), span=(1,1), flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(lbl_password, pos=(3,2), flag=wx.LEFT|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.Add(self.txt_ssh_pw, pos=(3,3), span=(1,1), flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL, border=5)
        dst_grid.AddGrowableCol(1)
        dst_grid.AddGrowableCol(3)
        dst_sizer.Add(dst_grid, flag=wx.EXPAND)
        sizer.Add(dst_sizer, pos=(0, 0), span=(1, 5), 
            flag=wx.EXPAND|wx.TOP|wx.LEFT|wx.RIGHT , border=0)
        # Run params
        run_box = wx.StaticBox(panel, label="Run")
        run_sizer = wx.StaticBoxSizer(run_box, wx.VERTICAL)
        run_grid = wx.GridSizer(3, 4, 5, 5)
        lbl_cell_id = wx.StaticText(panel, label="Flo-Cell ID")
        self.txt_cell_id = wx.TextCtrl(panel, style=wx.TE_RIGHT, size=(80, -1), value=self.app.cell_id)
        lbl_cell_type = wx.StaticText(panel, label="Flo-Cell Type")
        self.txt_cell_type = wx.TextCtrl(panel, style=wx.TE_RIGHT, size=(80, -1), value=self.app.cell_type)
        lbl_kit = wx.StaticText(panel, label="Sequencing Kit")
        self.txt_kit = wx.TextCtrl(panel, style=wx.TE_RIGHT, size=(80, -1), value=self.app.kit)
        lbl_usr1 = wx.StaticText(panel, label="User Field 1")
        self.txt_usr1 = wx.TextCtrl(panel, style=wx.TE_RIGHT, size=(60, -1), value=self.app.usr1)
        lbl_usr2 = wx.StaticText(panel, label="User Field 2")
        self.txt_usr2 = wx.TextCtrl(panel, style=wx.TE_RIGHT, size=(60, -1), value=self.app.usr2)
        run_grid.Add(lbl_cell_id, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        run_grid.Add(self.txt_cell_id)
        run_grid.Add(lbl_usr1, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        run_grid.Add(self.txt_usr1)
        run_grid.Add(lbl_cell_type, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        run_grid.Add(self.txt_cell_type)
        run_grid.Add(lbl_usr2, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        run_grid.Add(self.txt_usr2)
        run_grid.Add(lbl_kit, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        run_grid.Add(self.txt_kit)
        run_sizer.Add(run_grid, flag=wx.EXPAND)
        sizer.Add(run_sizer, pos=(1, 0), span=(1, 5), 
            flag=wx.EXPAND|wx.TOP|wx.LEFT|wx.RIGHT , border=0)
        # Options
        opt_box = wx.StaticBox(panel, label="Options")
        opt_sizer = wx.StaticBoxSizer(opt_box, wx.VERTICAL)
        opt_grid = wx.GridSizer(4, 4, 5, 5)
        self.chk_recursive = wx.CheckBox(panel, label="Recursive")
        self.chk_igexist = wx.CheckBox(panel, label="Ignore Existing")
        lbl_batch_size = wx.StaticText(panel, label="Batch size")
        lbl_batch_offset = wx.StaticText(panel, label="Batch offset")
        lbl_delay = wx.StaticText(panel, label="Delay")
        self.int_batch_size = intctrl.IntCtrl(panel, style=wx.TE_RIGHT, size=(80, -1), value=self.app.batch_size)
        self.int_batch_offset = intctrl.IntCtrl(panel, style=wx.TE_RIGHT, size=(60, -1), value=self.app.batch_offset)
        self.int_delay = intctrl.IntCtrl(panel, style=wx.TE_RIGHT, size=(80, -1), value=self.app.delay)
        lbl_file_regex = wx.StaticText(panel, label="File regex")
        lbl_batch_prefix = wx.StaticText(panel, label="Batch prefix")
        self.txt_file_regex = wx.TextCtrl(panel, style=wx.TE_RIGHT, size=(80, -1), value=self.app.regex)
        self.txt_batch_prefix = wx.TextCtrl(panel, style=wx.TE_RIGHT, size=(60, -1), value=self.app.batch_prefix)
        opt_grid.Add(self.chk_recursive, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(self.chk_igexist, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(wx.StaticText(panel))
        opt_grid.Add(wx.StaticText(panel))
        opt_grid.Add(lbl_batch_size, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(self.int_batch_size, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(lbl_batch_offset, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(self.int_batch_offset, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(lbl_file_regex, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(self.txt_file_regex, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(lbl_batch_prefix, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(self.txt_batch_prefix, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(lbl_delay, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_grid.Add(self.int_delay, flag=wx.ALIGN_CENTER_VERTICAL, border=5)
        opt_sizer.Add(opt_grid, flag=wx.EXPAND)
        sizer.Add(opt_sizer, pos=(2, 0), span=(1, 5), 
            flag=wx.EXPAND|wx.TOP|wx.LEFT|wx.RIGHT , border=0)
        # Log
        self.txt_log = wx.TextCtrl(panel, style=wx.TE_MULTILINE)
        self.txt_log.Disable()
        sizer.Add(self.txt_log, pos=(3,0), span=(1,5), flag=wx.EXPAND, border=5)
        # Flow Control
        self.btn_Start = wx.Button(panel, label="Start")
        sizer.Add(self.btn_Start, pos=(4, 3))
        self.btn_Stop = wx.Button(panel, label="Stop")
        self.btn_Stop.Disable()
        sizer.Add(self.btn_Stop, pos=(4, 4), span=(1, 1),  
            flag=wx.BOTTOM|wx.RIGHT, border=5)
        sizer.AddGrowableCol(2)
        sizer.AddGrowableRow(3)
        panel.SetSizer(sizer)
        self.panel = panel

    def initEvents(self):
        self.Bind(wx.EVT_CLOSE, self.on_exit)
        self.Bind(wx.EVT_BUTTON, self.on_source_browse, self.btn_Source)
        self.Bind(wx.EVT_BUTTON, self.on_destination_browse, self.btn_Destination)
        self.Bind(wx.EVT_BUTTON, self.on_key_browse, self.btn_ssh_key)
        self.Bind(wx.EVT_BUTTON, self.on_start_click, self.btn_Start)
        self.Bind(wx.EVT_BUTTON, self.on_stop_click, self.btn_Stop)

    def on_exit(self, event):
        self.on_stop_click(None)
        self.Destroy()

    def on_about(self, event):
        wx.MessageBox("Export MinION reads in batches to remote location.",
                      "MinION Export Deamon",
                      wx.OK|wx.ICON_INFORMATION)     

    def on_log(self, log):
        wx.CallAfter(self.txt_log.Enable)
        wx.CallAfter(self.txt_log.AppendText, log)
        wx.CallAfter(self.txt_log.AppendText, '\n')
        wx.CallAfter(self.txt_log.Disable)

    def on_source_browse(self, event):
        dlg = wx.DirDialog (None, "Choose source directory", "",
                    wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
        if dlg.ShowModal() == wx.ID_OK:
            self.txt_Source.SetValue(dlg.GetPath())

    def on_destination_browse(self, event):
        dlg = wx.DirDialog (None, "Choose destination directory", "",
                    wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
        if dlg.ShowModal() == wx.ID_OK:
            self.txt_Destination.SetValue(dlg.GetPath())

    def on_key_browse(self, event):
        dlg = wx.FileDialog (None, "Choose private key file",
                             style=wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        if dlg.ShowModal() == wx.ID_OK:
            self.txt_ssh_key.SetValue(dlg.GetPath())

    def on_start_click(self, event):
        self.app.source_path = self.txt_Source.GetValue()
        self.app.export_path = self.txt_Destination.GetValue()
        self.app.recursive = self.chk_recursive.GetValue()
        self.app.ignore_existing = self.chk_igexist.GetValue()
        self.app.batch_size = self.int_batch_size.GetValue()
        self.app.batch_offset = self.int_batch_offset.GetValue()
        self.app.delay = self.int_delay.GetValue()
        self.app.regex = self.txt_file_regex
        self.app.batch_prefix = self.txt_batch_prefix.GetValue()
        if self.app.is_startable():
            if self.app.start_watchdog():
                for child in self.panel.GetChildren():
                    if hasattr(child, 'Disable'):
                        child.Disable()
                self.btn_Stop.Enable()

    def on_stop_click(self, event):
        if self.app.stop_watchdog():
            for child in self.panel.GetChildren():
                if hasattr(child, 'Enable'):
                    child.Enable()
            self.btn_Stop.Disable()


if __name__ == '__main__':
    app = wx.App()
    app_main = app_window(None, title='NanoSCP')
    app.MainLoop()
    exit()