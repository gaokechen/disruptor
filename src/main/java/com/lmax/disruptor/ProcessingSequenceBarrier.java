/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;


/**
 * {@link SequenceBarrier} handed out for gating {@link EventProcessor}s on a cursor sequence and optional dependent {@link EventProcessor}(s),
 * using the given WaitStrategy.
 * {@link SequenceBarrier} 使用给定的 WaitStrategy 分发给游标序列上的门控 {@link EventProcessor} 和可选的依赖 {@link EventProcessor}。
 * 消费者通过 ProcessingSequenceBarrier 读取数据，实现了 SequenceBarrier 接口，用于和 RingBuffer 之间进行交互
 */
final class ProcessingSequenceBarrier implements SequenceBarrier
{
    private final WaitStrategy waitStrategy;
    private final Sequence dependentSequence;
    private volatile boolean alerted = false;
    private final Sequence cursorSequence;
    private final Sequencer sequencer;

    ProcessingSequenceBarrier(
        final Sequencer sequencer,
        final WaitStrategy waitStrategy,
        final Sequence cursorSequence,
        final Sequence[] dependentSequences)
    {
        this.sequencer = sequencer;
        this.waitStrategy = waitStrategy;
        this.cursorSequence = cursorSequence;
        if (0 == dependentSequences.length)
        {
            dependentSequence = cursorSequence;
        }
        else
        {
            dependentSequence = new FixedSequenceGroup(dependentSequences);
        }
    }

    @Override
    public long waitFor(final long sequence)
        throws AlertException, InterruptedException, TimeoutException
    {
        // 检查 alerted 异常
        checkAlert();

        // 通过 waitStrategy 获取可用的 availableSequence, cursorSequence 为当前的 Sequence, dependentSequence 为依赖的 Sequence
        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, dependentSequence, this);

        // availableSequence 比预期的 sequence 小，可能序号被重置回老的 oldSequence
        if (availableSequence < sequence)
        {
            return availableSequence;
        }

        /**
         * 获取可以从环形缓冲区安全读取的最大序列值。根据 Sequencer 的实现，此调用可能需要扫描 Sequencer 中的多个值。
         * 扫描范围从 nextSequence 到 availableSequence。如果没有比 nextSequence 大的可用序列，会返回nextSequence - 1。
         * 为了正常工作，消费者应该传递一个比成功处理的最后一个序列高 1 的值。
         * 会在多生产者中出现，当生产者1获取到序号13，生产者2获取到14；生产者1没发布，生产者2发布，会导致获取的可用序号为12，而 sequence 为13
         */
        return sequencer.getHighestPublishedSequence(sequence, availableSequence);
    }

    @Override
    public long getCursor()
    {
        return dependentSequence.get();
    }

    @Override
    public boolean isAlerted()
    {
        return alerted;
    }

    @Override
    public void alert()
    {
        alerted = true;
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void clearAlert()
    {
        alerted = false;
    }

    @Override
    public void checkAlert() throws AlertException
    {
        if (alerted)
        {
            throw AlertException.INSTANCE;
        }
    }
}