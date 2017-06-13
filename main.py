import time
import pathlib
import queue
import logging
import threading
import signal
import sys

import pyaudio
import configargparse
import humanfriendly

import cycle_writer

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

CHANNELS = 2
RATE = 48000
CHUNK = 1024
BUFFER_SECONDS = 1

OUT_FILE_SIZE = '1 GB'
OUT_TOTAL_MAX_SIZE = '10 GB'
OUTPUT_DIR = pathlib.Path('recordings')


class AsyncRecorder:
    def __init__(self, queue_buffer, media_format):
        self.media_format = media_format
        self.queue_buffer = queue_buffer
        self.stream = None
        self.audio = pyaudio.PyAudio()
        media_format.sample_size = self.audio.get_sample_size(media_format.format)

    def start_recording(self):
        self.stream = self.audio.open(format=self.media_format.format,
                                 channels=self.media_format.channels,
                                 rate=self.media_format.rate,
                                 input=True,
                                 frames_per_buffer=self.media_format.chunk,
                                 stream_callback=self.get_callback())
        while cycle_writer.continue_recording:
            time.sleep(0.1)
        self.close()

    def get_callback(self):
        def callback(in_data, frame_count, time_info, status):
            self.queue_buffer.put(in_data)
            return in_data, pyaudio.paContinue
        return callback

    def close(self):
        logger.debug('done capturing')
        self.stream.close()
        self.audio.terminate()


class MediaFormat:
    def __init__(self, channels=CHANNELS, rate=RATE, chunk=CHUNK, audio_format=pyaudio.paInt16, buffer_seconds=BUFFER_SECONDS):
        logger.debug('params {} {} {}'.format(channels, rate, chunk))
        self.channels = channels
        self.rate = rate
        self.chunk = chunk
        self.format = audio_format
        self.buffer_seconds = buffer_seconds
        self.sample_size = None


def writer_thread(writer):
    writer.start_writing()


def recorder_thread(recorder):
    recorder.start_recording()


def main():
    p = configargparse.ArgParser()
    p.add('-o', '--output', default='recordings', help='Output directory. Will be created')
    p.add('-t', '--max-size', default=OUT_TOTAL_MAX_SIZE, help='Total used space. Not byte accurate')
    p.add('-p', '--part-size', default=OUT_FILE_SIZE, help='Size to split recording. Not byte accurate')
    p.add('-D', '--debug', default=False, help='print debug messages')
    args = p.parse_args()

    queue_chunks_size = int(RATE / CHUNK * BUFFER_SECONDS * 2)
    queue_buffer = queue.Queue(maxsize=queue_chunks_size)
    media_format = MediaFormat()

    recorder = AsyncRecorder(queue_buffer, media_format)
    writer = cycle_writer.Writer(queue_buffer, media_format,
                                 humanfriendly.parse_size(args.max_size),
                                 humanfriendly.parse_size(args.part_size),
                                 pathlib.Path(args.output))

    tr = threading.Thread(target=recorder_thread, args=(recorder,))
    tr.start()

    tw = threading.Thread(target=writer_thread, args=(writer,))
    tw.start()
    while True:
        signal.pause()


def exit_gracefully(signum, frame):
    # restore the original signal handler as otherwise evil things will happen
    # in raw_input when CTRL+C is pressed, and our signal handler is not re-entrant
    signal.signal(signal.SIGINT, original_sigint)

    cycle_writer.continue_recording = False
    logger.debug('exit')
    sys.exit(0)

if __name__ == '__main__':
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)
    main()
