import os
import wave
import time
import logging
import subprocess
import fcntl
import threading

logger = logging.getLogger()

OUTPUT_NAME_FORMAT = 'recording'

continue_recording = True


class Writer:
    def __init__(self, queue_buffer, media_format, total_max_size, max_file_size, output_dir):
        self.queue_buffer = queue_buffer
        self.media_format = media_format
        self.total_max_size = total_max_size
        self.max_file_size = max_file_size
        self.used_size = 0
        self.tracked_files = []
        self.out_dir = output_dir
        self.recording_file = None
        self.calculate_existing_files_size()

    def calculate_existing_files_size(self):
        pattern = OUTPUT_NAME_FORMAT + '*.opus'
        logger.debug(pattern)
        existing_files = self.out_dir.glob(pattern)

        existing_files = [FileAndSize(f) for f in existing_files]
        existing_files.sort(key=lambda x: x.file.stat().st_mtime)

        for f in existing_files:
            self.used_size += f.size

        self.tracked_files = existing_files[:]
        logger.debug('existing files {}'.format(len(existing_files)))

    def start_writing(self):
        self.rotate_file()

        while continue_recording:
            time.sleep(0.1)
            if self.queue_buffer.qsize() > self.queue_buffer.maxsize // 2:
                raw_data = b''
                while not self.queue_buffer.empty():
                    raw_data += self.queue_buffer.get()

                if self.recording_file.size > self.max_file_size:
                    self.rotate_file()
                self.recording_file.write(raw_data)

        if self.recording_file is not None:
            self.recording_file.close()
        logger.debug('done recording')

    def rotate_file(self):
        if self.recording_file is not None:
            self.recording_file.close()
            self.tracked_files.append(FileAndSize(self.recording_file.name, self.recording_file.size))
            self.used_size += self.recording_file.size

        name = OUTPUT_NAME_FORMAT + ' {}.opus'.format(time.ctime())
        name = self.out_dir / name
        self.out_dir.mkdir(parents=True, exist_ok=True)
        logger.info('current out file name:{} total used size {:,d}'.format(name, self.used_size))

        self.recording_file = CountedLenOpusFile(name, self.media_format)

        while self.used_size > self.total_max_size:
            self.remove_old_files()

    def remove_old_files(self):
        oldest_recording = self.tracked_files[0]

        try:
            os.unlink(oldest_recording.file)
        except FileNotFoundError:
            logger.debug('{} already deleted'.format(oldest_recording.file))
        self.used_size -= oldest_recording.size
        del self.tracked_files[0]
        logger.debug('deleted old file {} {} {}'.format(oldest_recording.file, oldest_recording.size, self.used_size))


class CountedLenOpusFile:
    def __init__(self, name, media_format):
        self.name = name
        self.media_format = media_format

        self.make_pipe('out_pipe')
        self.make_pipe('in_pipe')
        self.proc = subprocess.Popen(['opusenc', '--raw', '--raw-bits', '16', '--raw-rate', str(self.media_format.rate),
                                      '--raw-chan', str(self.media_format.channels),
                                      'in_pipe', 'out_pipe'], )

        self.in_pipe = open('in_pipe', 'wb')
        self.out_pipe = open('out_pipe', 'rb')

        # non blocking read
        file_flags = fcntl.fcntl(self.out_pipe, fcntl.F_GETFL)
        fcntl.fcntl(self.out_pipe, fcntl.F_SETFL, file_flags | os.O_NDELAY)

        self.size = 0
        self.opus_file = open(name, 'wb')

        self.write_event = threading.Event()
        self.read_thread = threading.Thread(target=self.read, args=(self.write_event,))
        self.read_thread.start()

    def write(self, data):
        self.in_pipe.write(data)
        self.write_event.set()

    def read(self, e):
        while continue_recording:
            e.wait()
            enc_data = self.out_pipe.read()
            self.__write(enc_data)

    def __write(self, data):
        if data is not None:
            self.size += len(data)
            self.opus_file.write(data)

    def close(self):
        self.proc.communicate()
        enc_data = self.out_pipe.read()

        self.write_event.set()
        self.read_thread.join()

        self.in_pipe.close()
        self.out_pipe.close()

        self.__write(enc_data)
        self.opus_file.close()

    def make_pipe(self, name):
        if os.path.exists(name):
            os.unlink(name)

        if not os.path.exists(name):
            os.mkfifo(name)
            return

    
class LimitedLenWawFile:
    def __init__(self, name, media_format):
        self.name = name
        self.waveFile = wave.open(str(name), 'wb')
        self.waveFile.setnchannels(media_format.channels)
        self.waveFile.setsampwidth(media_format.sample_size)
        self.waveFile.setframerate(media_format.rate)
        self.size = 0

    def write(self, data):
        self.waveFile.writeframes(data)
        self.size += len(data)

    def close(self):
        self.waveFile.close()


class FileAndSize:
    def __init__(self, file, size=None):
        self.file = file
        self._size = size

    @property
    def size(self):
        if self._size is None:
            self._size = self.file.stat().st_size
        return self._size
