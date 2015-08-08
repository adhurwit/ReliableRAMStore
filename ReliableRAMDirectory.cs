/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Lucene.Net.Support;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Lucene.Net.Store
{

    /// <summary> A memory-resident <see cref="Directory"/> implementation.  Locking
    /// implementation is by default the <see cref="SingleInstanceLockFactory"/>
    /// but can be changed with <see cref="Directory.SetLockFactory"/>.
	/// </summary>
	[Serializable]
	public class ReliableRAMDirectory: Lucene.Net.Store.Directory
    {
        private const long serialVersionUID = 1L;

		internal protected long internalSizeInBytes = 0;

        public static IReliableDictionary<string, ReliableRAMFile> fileMap
        {
            get
            {
                return StateManager.GetOrAddAsync<IReliableDictionary<string, ReliableRAMFile>>("TheIndices").Result;
            }
        }

        public static IReliableStateManager StateManager
        {
            get;
            set;
        }


        public static void UpdateFile(ReliableRAMFile file)
        {
            var existing = GetFile(file.MapName);
            using (var tx = StateManager.CreateTransaction())
            {
                fileMap.TryUpdateAsync(tx, file.MapName, file, existing);
                tx.CommitAsync().GetAwaiter().GetResult();
            }
        }

        // *****
        // Lock acquisition sequence:  RAMDirectory, then RAMFile
        // *****

            /// <summary>Constructs an empty <see cref="Directory"/>. </summary>
        public ReliableRAMDirectory()
		{
            SetLockFactory(new SingleInstanceLockFactory());
		}


        /// <summary> Creates a new <c>RAMDirectory</c> instance from a different
        /// <c>Directory</c> implementation.  This can be used to load
        /// a disk-based index into memory.
        /// <p/>
        /// This should be used only with indices that can fit into memory.
        /// <p/>
        /// Note that the resulting <c>RAMDirectory</c> instance is fully
        /// independent from the original <c>Directory</c> (it is a
        /// complete copy).  Any subsequent changes to the
        /// original <c>Directory</c> will not be visible in the
        /// <c>RAMDirectory</c> instance.
        /// 
        /// </summary>
        /// <param name="dir">a <c>Directory</c> value
        /// </param>
        /// <exception cref="System.IO.IOException">if an error occurs
        /// </exception>
        public ReliableRAMDirectory(Directory dir):this(dir, false)
		{
		}

        private ReliableRAMDirectory(Directory dir, bool closeDir)
            : this()
		{
			Directory.Copy(dir, this, closeDir);
		}

         //https://issues.apache.org/jira/browse/LUCENENET-174
        [System.Runtime.Serialization.OnDeserialized]
        void OnDeserialized(System.Runtime.Serialization.StreamingContext context)
        {
            if (interalLockFactory == null)
            {
                SetLockFactory(new SingleInstanceLockFactory());
            }
        }
		
		public override System.String[] ListAll()
		{
			lock (this)
			{
				EnsureOpen();
                List<string> keys = new List<string>();
                foreach (var kv in fileMap.ToList())
                    keys.Add(kv.Key);

                System.Collections.Generic.ISet<string> fileNames = 
                    Lucene.Net.Support.Compatibility.SetFactory.CreateHashSet(keys);
				System.String[] result = new System.String[fileNames.Count];
				int i = 0;
				foreach(string filename in fileNames)
				{
                    result[i++] = filename;
				}
				return result;
			}
		}
		
		/// <summary>Returns true iff the named file exists in this directory. </summary>
		public override bool FileExists(System.String name)
		{
			EnsureOpen();
            using (var tx = StateManager.CreateTransaction())
            {
                var ret = fileMap.ContainsKeyAsync(tx, name).Result;
                tx.CommitAsync().GetAwaiter().GetResult();
                return ret;
            }

		}

        static ReliableRAMFile GetFile(string name)
        {
            ReliableRAMFile file;
            using (var tx = StateManager.CreateTransaction())
            {
                file = fileMap.TryGetValueAsync(tx, name).GetAwaiter().GetResult().Value;
                tx.CommitAsync().GetAwaiter().GetResult();
            }
            if(file != null)
                file.MapName = name;

            return file;
        }

        /// <summary>Returns the time the named file was last modified.</summary>
        /// <throws>  IOException if the file does not exist </throws>
        public override long FileModified(System.String name)
		{
			EnsureOpen();
            ReliableRAMFile file = GetFile(name);
            if (file == null)
            {
                throw new System.IO.FileNotFoundException(name);
            }

            // RAMOutputStream.Flush() was changed to use DateTime.UtcNow.
            // Convert it back to local time before returning (previous behavior)
            return new DateTime(file.LastModified*TimeSpan.TicksPerMillisecond, DateTimeKind.Utc).ToLocalTime().Ticks/
		           TimeSpan.TicksPerMillisecond;
		}
		
		/// <summary>Set the modified time of an existing file to now.</summary>
		/// <throws>  IOException if the file does not exist </throws>
		public override void  TouchFile(System.String name)
		{
			EnsureOpen();
            ReliableRAMFile file = GetFile(name);
            if (file == null)
            {
                throw new System.IO.FileNotFoundException(name);
            }

            long ts2, ts1 = System.DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
			do 
			{
				try
				{
					System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 0 + 100 * 1));
				}
				catch (System.Threading.ThreadInterruptedException ie)
				{
					// In 3.0 we will change this to throw
					// InterruptedException instead
					ThreadClass.Current().Interrupt();
					throw new System.SystemException(ie.Message, ie);
				}
                ts2 = System.DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
			}
			while (ts1 == ts2);
			
			file.LastModified = ts2;
		}
		
		/// <summary>Returns the length in bytes of a file in the directory.</summary>
		/// <throws>  IOException if the file does not exist </throws>
		public override long FileLength(System.String name)
		{
			EnsureOpen();
            var file = GetFile(name);
            if (file == null)
            {
                throw new System.IO.FileNotFoundException(name);
            }
            return file.Length;

		}
		
		/// <summary>Return total size in bytes of all files in this
		/// directory.  This is currently quantized to
		/// RAMOutputStream.BUFFER_SIZE. 
		/// </summary>
		public long SizeInBytes()
		{
            lock(this)
            {
                return internalSizeInBytes;
            }
		}
		
		/// <summary>Removes an existing file in the directory.</summary>
		/// <throws>  IOException if the file does not exist </throws>
		public override void DeleteFile(System.String name)
		{
            lock(this)
            {
                EnsureOpen();
                ReliableRAMFile file = GetFile(name);
                if (file == null)
                {
                    throw new System.IO.FileNotFoundException(name);
                }

                using (var tx = StateManager.CreateTransaction())
                {
                    fileMap.TryRemoveAsync(tx, name).GetAwaiter().GetResult();
                    tx.CommitAsync().GetAwaiter().GetResult();
                }
                file.directory = null;
                internalSizeInBytes -= file.sizeInBytes;
            }
		}
		
		/// <summary>Creates a new, empty file in the directory with the given name. Returns a stream writing this file. </summary>
		public override IndexOutput CreateOutput(System.String name)
		{
			EnsureOpen();
            ReliableRAMFile file = new ReliableRAMFile(this, name);
            ReliableRAMFile existing = GetFile(name);
            using (var tx = StateManager.CreateTransaction())
            {
                if (existing != null)
                {
                    internalSizeInBytes -= existing.sizeInBytes;
                    existing.directory = null;
                    fileMap.TryUpdateAsync(tx, name, file, existing).GetAwaiter().GetResult();
                }
                else
                {
                    fileMap.TryAddAsync(tx, name, file).GetAwaiter().GetResult();
                }
                tx.CommitAsync().GetAwaiter().GetResult();
            }
            return new ReliableRAMOutputStream(file);
		}
		
		/// <summary>Returns a stream reading an existing file. </summary>
		public override IndexInput OpenInput(System.String name)
		{
			EnsureOpen();
            ReliableRAMFile file = GetFile(name);
            if (file == null)
            {
                  throw new System.IO.FileNotFoundException(name);
            }

            return new ReliableRAMInputStream(file);
		}

        /// <summary>Closes the store to future operations, releasing associated memory. </summary>
        protected override async void Dispose(bool disposing)
        {
            isOpen = false;
            await fileMap.ClearAsync();
        }

        //public HashMap<string, RAMFile> fileMap_ForNUnit
        //{
        //    get { return fileMap; }
        //}

        //public long sizeInBytes_ForNUnitTest
        //{
        //    get { return sizeInBytes; }
        //    set { sizeInBytes = value; }
        //}
	}
}