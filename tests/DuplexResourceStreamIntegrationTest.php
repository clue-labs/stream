<?php

namespace React\Tests\Stream;

use React\Stream\DuplexResourceStream;
use React\Stream\ReadableResourceStream;
use React\Stream\ReadableStreamInterface;
use React\EventLoop\ExtEventLoop;
use React\EventLoop\ExtLibeventLoop;
use React\EventLoop\ExtLibevLoop;
use React\EventLoop\LoopInterface;
use React\EventLoop\LibEventLoop;
use React\EventLoop\LibEvLoop;
use React\EventLoop\StreamSelectLoop;

class DuplexResourceStreamIntegrationTest extends TestCase
{
    public function loopProvider()
    {
        return array(
            array(
                function() {
                    return true;
                },
                function () {
                    return new StreamSelectLoop();
                }
            ),
            array(
                function () {
                    return function_exists('event_base_new');
                },
                function () {
                    return class_exists('React\EventLoop\ExtLibeventLoop') ? new ExtLibeventLoop() : new LibEventLoop();
                }
            ),
            array(
                function () {
                    return class_exists('libev\EventLoop');
                },
                function () {
                    return class_exists('React\EventLoop\ExtLibevLoop') ? new ExtLibevLoop() : new LibEvLoop();
                }
            ),
            array(
                function () {
                    return class_exists('EventBase') && class_exists('React\EventLoop\ExtEventLoop');
                },
                function () {
                    return new ExtEventLoop();
                }
            )
        );
    }

    /**
     * @dataProvider loopProvider
     */
    public function testBufferReadsLargeChunks($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        list($sockA, $sockB) = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

        $bufferSize = 4096;
        $streamA = new DuplexResourceStream($sockA, $loop, $bufferSize);
        $streamB = new DuplexResourceStream($sockB, $loop, $bufferSize);

        $testString = str_repeat("*", $bufferSize + 1);

        $buffer = "";
        $streamB->on('data', function ($data) use (&$buffer) {
            $buffer .= $data;
        });

        $streamA->write($testString);

        $this->loopTick($loop);
        $this->loopTick($loop);
        $this->loopTick($loop);

        $streamA->close();
        $streamB->close();

        $this->assertEquals($testString, $buffer);
    }

    /**
     * @dataProvider loopProvider
     */
    public function testWriteLargeChunk($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        list($sockA, $sockB) = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

        $streamA = new DuplexResourceStream($sockA, $loop);
        $streamB = new DuplexResourceStream($sockB, $loop);

        // limit seems to be 192 KiB
        $size = 256 * 1024;

        // sending side sends and expects clean close with no errors
        $streamA->end(str_repeat('*', $size));
        $streamA->on('close', $this->expectCallableOnce());
        $streamA->on('error', $this->expectCallableNever());

        // receiving side counts bytes and expects clean close with no errors
        $received = 0;
        $streamB->on('data', function ($chunk) use (&$received) {
            $received += strlen($chunk);
        });
        $streamB->on('error', $this->expectCallableNever());

        $this->awaitStreamClose($streamB, $loop);

        $streamA->close();
        $streamB->close();

        $this->assertEquals($size, $received);
    }

    /**
     * @dataProvider loopProvider
     */
    public function testDoesNotEmitDataIfNothingHasBeenWritten($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        list($sockA, $sockB) = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

        $streamA = new DuplexResourceStream($sockA, $loop);
        $streamB = new DuplexResourceStream($sockB, $loop);

        // end streamA without writing any data
        $streamA->end();

        // streamB should not emit any data
        $streamB->on('data', $this->expectCallableNever());

        $this->awaitStreamClose($streamB, $loop);
    }

    /**
     * @dataProvider loopProvider
     */
    public function testDoesNotWriteDataIfRemoteSideFromPairHasBeenClosed($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        list($sockA, $sockB) = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);

        $streamA = new DuplexResourceStream($sockA, $loop);
        $streamB = new DuplexResourceStream($sockB, $loop);

        // try to write data to closed remote end
        $streamA->pause();
        $streamA->write('hello');
        $streamA->on('error', $this->expectCallableOnce());

        $streamB->on('data', $this->expectCallableNever());
        $streamB->close();

        $this->awaitStreamClose($streamA, $loop);

        $streamA->close();
        $streamB->close();
    }

    /**
     * @dataProvider loopProvider
     */
    public function testDoesNotWriteDataIfServerSideHasBeenClosed($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        $server = stream_socket_server('tcp://127.0.0.1:0');

        $client = stream_socket_client(stream_socket_get_name($server, false));
        $peer = stream_socket_accept($server);

        $streamA = new DuplexResourceStream($client, $loop);
        $streamB = new DuplexResourceStream($peer, $loop);

        // end streamA without writing any data
        $streamA->pause();
        $streamA->write('hello');
        $streamA->on('error', $this->expectCallableOnce());

        $streamB->on('data', $this->expectCallableNever());
        $streamB->close();

        $this->awaitStreamClose($streamA, $loop);

        $streamA->close();
        $streamB->close();
    }

    /**
     * @dataProvider loopProvider
     */
    public function testDoesNotWriteDataIfClientSideHasBeenClosed($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        $server = stream_socket_server('tcp://127.0.0.1:0');

        $client = stream_socket_client(stream_socket_get_name($server, false));
        $peer = stream_socket_accept($server);

        $streamA = new DuplexResourceStream($peer, $loop);
        $streamB = new DuplexResourceStream($client, $loop);

        // end streamA without writing any data
        $streamA->pause();
        $streamA->write('hello');
        $streamA->on('error', $this->expectCallableOnce());

        $streamB->on('data', $this->expectCallableNever());
        $streamB->close();

        $this->awaitStreamClose($streamA, $loop);

        $streamA->close();
        $streamB->close();
    }

    /**
     * @dataProvider loopProvider
     */
    public function testReadsSingleChunkFromProcessPipe($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        $stream = new ReadableResourceStream(popen('echo test', 'r'), $loop);
        $stream->on('data', $this->expectCallableOnceWith("test\n"));
        $stream->on('end', $this->expectCallableOnce());
        $stream->on('error', $this->expectCallableNever());

        $this->awaitStreamClose($stream, $loop);
    }

    /**
     * @dataProvider loopProvider
     */
    public function testReadsMultipleChunksFromProcessPipe($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        $stream = new ReadableResourceStream(popen('echo a;sleep 0.1;echo b;sleep 0.1;echo c', 'r'), $loop);

        $buffer = '';
        $stream->on('data', function ($chunk) use (&$buffer) {
            $buffer .= $chunk;
        });

        $stream->on('end', $this->expectCallableOnce());
        $stream->on('error', $this->expectCallableNever());

        $this->awaitStreamClose($stream, $loop);

        $this->assertEquals("a\n" . "b\n" . "c\n", $buffer);
    }

    /**
     * @dataProvider loopProvider
     */
    public function testReadsLongChunksFromProcessPipe($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        $stream = new ReadableResourceStream(popen('dd if=/dev/zero bs=12345 count=1234 2>&-', 'r'), $loop);

        $bytes = 0;
        $stream->on('data', function ($chunk) use (&$bytes) {
            $bytes += strlen($chunk);
        });

        $stream->on('end', $this->expectCallableOnce());
        $stream->on('error', $this->expectCallableNever());

        $this->awaitStreamClose($stream, $loop);

        $this->assertEquals(12345 * 1234, $bytes);
    }

    /**
     * @dataProvider loopProvider
     */
    public function testReadsNothingFromProcessPipeWithNoOutput($condition, $loopFactory)
    {
        if (true !== $condition()) {
            return $this->markTestSkipped('Loop implementation not available');
        }

        $loop = $loopFactory();

        $stream = new ReadableResourceStream(popen('true', 'r'), $loop);
        $stream->on('data', $this->expectCallableNever());
        $stream->on('end', $this->expectCallableOnce());
        $stream->on('error', $this->expectCallableNever());

        $this->awaitStreamClose($stream, $loop);
    }

    private function loopTick(LoopInterface $loop)
    {
        $loop->addTimer(0, function () use ($loop) {
            $loop->stop();
        });
        $loop->run();
    }

    private function awaitStreamClose(ReadableStreamInterface $stream, LoopInterface $loop, $timeout = 10.0)
    {
        $stream->on('close', function () use ($loop) {
            $loop->stop();
        });

        $that = $this;
        $loop->addTimer($timeout, function () use ($loop, $that) {
            $loop->stop();
            $that->fail('Timed out while waiting for stream to close');
        });

        $loop->run();
    }
}
