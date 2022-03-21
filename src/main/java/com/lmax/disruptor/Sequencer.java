/*
 * Copyright 2012 LMAX Ltd.
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
 * Coordinates claiming sequences for access to a data structure while tracking dependent {@link Sequence}s
 */
public interface Sequencer extends Cursored, Sequenced
{
    /**
     * Set to -1 as sequence starting point
     * 设置-1作为序列起点
     */
    long INITIAL_CURSOR_VALUE = -1L;

    /**
     * Claim a specific sequence.  Only used if initialising the ring buffer to
     * a specific value.
     * 声明一个具体的序列，仅在将环形缓冲区初始化为特定值时使用。
     *
     * @param sequence The sequence to initialise too.
     */
    void claim(long sequence);

    /**
     * Confirms if a sequence is published and the event is available for use; non-blocking.
     * 确认一个序列是否已发布，并且发布到序列上的事件是可处理的；非阻塞方法。
     *
     * @param sequence of the buffer to check
     * @return true if the sequence is available for use, false if not
     */
    boolean isAvailable(long sequence);

    /**
     * Add the specified gating sequences to this instance of the Disruptor.  They will
     * safely and atomically added to the list of gating sequences.
     * 添加一些控制序列到当前实例，添加过程是安全和原子的
     *
     * @param gatingSequences The sequences to add.
     */
    void addGatingSequences(Sequence... gatingSequences);

    /**
     * Remove the specified sequence from this sequencer.
     * 从sequencer中移除指定序列
     *
     * @param sequence to be removed.
     * @return <tt>true</tt> if this sequence was found, <tt>false</tt> otherwise.
     */
    boolean removeGatingSequence(Sequence sequence);

    /**
     * Create a new SequenceBarrier to be used by an EventProcessor to track which messages
     * are available to be read from the ring buffer given a list of sequences to track.
     * 创建一个新的 SequenceBarrier 以供 EventProcessor 使用，以跟踪哪些消息可从给定要跟踪的序列列表的环形缓冲区中读取。
     * 在判断ring buffer上某个事件是否能处理时使用
     *
     * @param sequencesToTrack All of the sequences that the newly constructed barrier will wait on.
     * @return A sequence barrier that will track the specified sequences.
     * @see SequenceBarrier
     */
    SequenceBarrier newBarrier(Sequence... sequencesToTrack);

    /**
     * Get the minimum sequence value from all of the gating sequences
     * added to this ringBuffer.
     * 从添加到此 ringBuffer 的所有控制序列中获取最小序列值。
     *
     * @return The minimum gating sequence or the cursor sequence if
     * no sequences have been added.
     */
    long getMinimumSequence();

    /**
     * Get the highest sequence number that can be safely read from the ring buffer.  Depending
     * on the implementation of the Sequencer this call may need to scan a number of values
     * in the Sequencer.  The scan will range from nextSequence to availableSequence.  If
     * there are no available values <code>&gt;= nextSequence</code> the return value will be
     * <code>nextSequence - 1</code>.  To work correctly a consumer should pass a value that
     * is 1 higher than the last sequence that was successfully processed.
     * 获取可以从环形缓冲区安全读取的最大序列值。根据 Sequencer 的实现，此调用可能需要扫描 Sequencer 中的多个值。
     * 扫描范围从 nextSequence 到 availableSequence。如果没有比nextSequence大的可用序列，会返回nextSequence - 1。
     * 为了正常工作，消费者应该传递一个比成功处理的最后一个序列高 1 的值。
     *
     * @param nextSequence      The sequence to start scanning from.
     * @param availableSequence The sequence to scan to.
     * @return The highest value that can be safely read, will be at least <code>nextSequence - 1</code>.
     */
    long getHighestPublishedSequence(long nextSequence, long availableSequence);

    /**
     * 通过给定的数据提供者和控制序列来创建一个EventPoller
     *
     * @param provider
     * @param gatingSequences
     * @param <T>
     * @return
     */
    <T> EventPoller<T> newPoller(DataProvider<T> provider, Sequence... gatingSequences);
}