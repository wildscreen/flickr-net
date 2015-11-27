using System;
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using System.Data.SQLite;

namespace FlickrNet
{
    /// <summary>
    /// A threadsafe cache that is backed by disk storage.
    /// 
    /// </summary>
    public sealed class SQLiteCache : IDisposable, ICacheStore
    {
        // The in-memory representation of the cache.
        // Use SortedList instead of Hashtable only to maintain backward 
        // compatibility with previous serialization scheme.  If we
        // abandon backward compatibility, we should switch to Hashtable.
        //private Dictionary<string, ICacheItem> dataTable = new Dictionary<string, ICacheItem>();

        private System.Data.IDbConnection connection;
        private string provider = "System.Data.SQLite, SQLite";
        private string tableName = "FlickrResponseCache";

        private readonly CacheItemPersister persister;

        // true if dataTable contains changes vs. on-disk representation
        private bool dirty;

        // The persistent file representation of the cache.
        private readonly FileInfo dataFile;
        private DateTime timestamp;  // last-modified time of dataFile when cache data was last known to be in sync
        private long length;         // length of dataFile when cache data was last known to be in sync

        //// The file-based mutex.  Named (dataFile.FullName + ".lock")
        //private readonly LockFile lockFile;

        /// <summary>
        /// Initializes a new instance of the <see cref="PersistentCache"/> for the given filename and cache type.
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="persister"></param>
        public SQLiteCache(string filename, CacheItemPersister persister)
        {
            this.persister = persister;
            dataFile = new FileInfo(filename + ".sqlite");
            //lockFile = new LockFile(filename + ".lock");
            connection = new SQLiteConnection("Data Source=" + dataFile.FullName + ";Version=3;");
            connection.Open();
            using (var cmd = connection.CreateCommand()) {
                cmd.CommandText = "create table if not exists " + tableName + " (key TEXT PRIMARY KEY, value TEXT, creationTime INTEGER)";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "create unique index if not exists " + tableName + "CreationTime ON " + tableName + "(creationTime)";
                cmd.ExecuteNonQuery();
            }

            //create table if not exists TableName (col1 typ1, ..., colN typN)
            

        }

        ///// <summary>
        ///// Return all items in the cache.  Works similarly to
        ///// ArrayList.ToArray(Type).
        ///// </summary>
        //public ICacheItem[] ToArray(Type valueType)
        //{
        //    using (lockFile.Acquire())
        //    {
        //        Refresh();
        //        string[] keys;
        //        Array values;
        //        InternalGetAll(valueType, out keys, out values);
        //        return (ICacheItem[])values;
        //    }
        //}

        /// <summary>
        /// Sets cache values.
        /// </summary>
        public ICacheItem this[string key]
        {
            set
            {
                if (key == null)
                    throw new ArgumentNullException("key");

                ICacheItem oldItem;

                //using (lockFile.Acquire())
                //{
                    //Refresh();
                    oldItem = InternalSet(key, value);
                    //Persist();
                //}
                if (oldItem != null)
                    oldItem.OnItemFlushed();
            }
        }

        /// <summary>
        /// Gets cache values.
        /// </summary>
        public ICacheItem this[string key, TimeSpan maxAge, bool removeIfExpired]
        {
            get {
                return Get(key, maxAge, removeIfExpired);
            }
        }

        /// <summary>
        /// Gets the specified key from the cache unless it has expired.
        /// </summary>
        /// <param name="key">The key to look up in the cache.</param>
        /// <param name="maxAge">The maximum age the item can be before it is no longer returned.</param>
        /// <param name="removeIfExpired">Whether to delete the item if it has expired or not.</param>
        /// <returns></returns>
        public ICacheItem Get(string key, TimeSpan maxAge, bool removeIfExpired)
        {
            Debug.Assert(maxAge > TimeSpan.Zero || maxAge == TimeSpan.MinValue, "maxAge should be positive, not negative");

            ICacheItem item;
            bool expired;
            //using (lockFile.Acquire())
            //{
                //Refresh();

                item = InternalGet(key);
                expired = item != null && Expired(item.CreationTime, maxAge);
                if (expired)
                {
                    if (removeIfExpired)
                    {
                        item = RemoveKey(key);
                        //Persist();
                    }
                    else
                        item = null;
                }
            //}

            if (expired && removeIfExpired)
                item.OnItemFlushed();

            return expired ? null : item;
        }

        /// <summary>
        /// Clears the current cache of items.
        /// </summary>
        public void Flush()
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "delete from " + tableName;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "vacuum";
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Shrinks the current cache to a specific size, removing older items first.
        /// </summary>
        /// <param name="size"></param>
        public void Shrink(long size)
        {
            if (size < 0)
                throw new ArgumentException("Cannot shrink to a negative size", "size");

            //SQLite files are 4K minimum
            long minSize = 4096;

            if (size <= minSize || dataFile.Length <= minSize || dataFile.Length <= size) return;

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "select Count(*) from " + tableName;
                var rows = long.Parse(cmd.ExecuteScalar().ToString());
                var avgRowLen = dataFile.Length / rows;
                var rowsToDel = (dataFile.Length - size) / avgRowLen;
                if (rowsToDel < 1) return;

                cmd.CommandText = "DELETE FROM " + tableName + " WHERE ROWID IN (SELECT ROWID FROM " + tableName + " ORDER BY ROWID ASC LIMIT " + rowsToDel + ")";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "vacuum";
                cmd.ExecuteNonQuery();
            }
        }

        private static bool Expired(DateTime test, TimeSpan age)
        {
            if (age == TimeSpan.MinValue)
                return true;
            else if (age == TimeSpan.MaxValue)
                return false;
            else
                return test < DateTime.UtcNow - age;
        }


        //private void InternalGetAll(Type valueType, out string[] keys, out Array values)
        //{
        //    if (!typeof(ICacheItem).IsAssignableFrom(valueType))
        //        throw new ArgumentException("Type " + valueType.FullName + " does not implement ICacheItem", "valueType");

        //    keys = new List<string>(dataTable.Keys).ToArray();
        //    values = Array.CreateInstance(valueType, keys.Length);
        //    for (int i = 0; i < keys.Length; i++)
        //        values.SetValue(dataTable[keys[i]], i);

        //    Array.Sort(values, keys, new CreationTimeComparer());
        //}

        private ICacheItem InternalGet(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "select key, value, creationTime from " + tableName + " where key=@key";
                cmd.Parameters.Add(new SQLiteParameter("@key", key));

                using (var reader = cmd.ExecuteReader()) {
                    if (reader.Read())
                    {
                        return new ResponseCacheItem(new Uri(key), reader.GetString(1), new DateTime(reader.GetInt64(2)));
                    }
                    else
                    {
                        return null;
                    }
                }
            }
        }

        /// <returns>The old value associated with <c>key</c>, if any.</returns>
        private ICacheItem InternalSet(string key, ICacheItem value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ICacheItem flushedItem;

            flushedItem = RemoveKey(key);
            if (value != null) // don't ever let nulls get in
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "replace into " + tableName + " values(@key, @value, @creationTime)";
                    cmd.Parameters.Add(new SQLiteParameter("@key", key));
                    cmd.Parameters.Add(new SQLiteParameter("@value", ((ResponseCacheItem)value).Response));
                    cmd.Parameters.Add(new SQLiteParameter("@creationTime", value.CreationTime.Ticks));

                    cmd.ExecuteNonQuery();
                }
            }

            //dirty = dirty || !object.ReferenceEquals(flushedItem, value);

            return flushedItem;
        }

        private ICacheItem RemoveKey(string key)
        {
            //if (!dataTable.ContainsKey(key)) return null;

            var cacheItem = InternalGet(key);
            if (cacheItem == null) return null;

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "delete from " + tableName + " where key=@key";
                cmd.Parameters.Add(new SQLiteParameter("@key", key));
                cmd.ExecuteNonQuery();
            }

            //dataTable.Remove(key);
            //dirty = true;
            return cacheItem;
        }

        //private void Refresh()
        //{
        //    Debug.Assert(!dirty, "Refreshing even though cache is dirty");

        //    DateTime newTimestamp = DateTime.MinValue;
        //    long newLength = -1;
            
        //    dataFile.Refresh();

        //    if (dataFile.Exists)
        //    {
        //        newTimestamp = dataFile.LastWriteTime;
        //        newLength = dataFile.Length;
        //    }

        //    if (timestamp != newTimestamp || length != newLength)
        //    {
        //        // file changed
        //        if (!dataFile.Exists)
        //            dataTable.Clear();
        //        else
        //        {
        //            Debug.WriteLine("Loading cache from disk");
        //            using (FileStream inStream = dataFile.Open(FileMode.Open, FileAccess.Read, FileShare.Read))
        //            {
        //                dataTable = Load(inStream);
        //            }
        //        }
        //    }

        //    timestamp = newTimestamp;
        //    length = newLength;
        //    dirty = false;
        //}

        //private void Persist()
        //{
        //    if (!dirty)
        //        return;

        //    Debug.WriteLine("Saving cache to disk");
        //    using (FileStream outStream = dataFile.Open(FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
        //    {
        //        Store(outStream, dataTable);
        //    }

        //    dataFile.Refresh();
        //    timestamp = dataFile.LastWriteTime;
        //    length = dataFile.Length;

        //    dirty = false;
        //}

        //private Dictionary<string, ICacheItem> Load(Stream s)
        //{
        //    var table = new Dictionary<string, ICacheItem>();
        //    int itemCount = UtilityMethods.ReadInt32(s);
        //    for (int i = 0; i < itemCount; i++)
        //    {
        //        try
        //        {
        //            string key = UtilityMethods.ReadString(s);
        //            ICacheItem val = persister.Read(s);
        //            if (val == null) // corrupt cache file 
        //                return table;

        //            table[key] = val;
        //        }
        //        catch (IOException)
        //        {
        //            return table;
        //        }
        //    }
        //    return table;
        //}

        //private void Store(Stream s, Dictionary<string, ICacheItem> table)
        //{
        //    UtilityMethods.WriteInt32(s, table.Count);
        //    foreach (KeyValuePair<string, ICacheItem> entry in table)
        //    {
        //        UtilityMethods.WriteString(s, (string)entry.Key);
        //        persister.Write(s, (ICacheItem)entry.Value);
        //    }
        //}

        private class CreationTimeComparer : IComparer
        {
            public int Compare(object x, object y)
            {
                return ((ICacheItem)x).CreationTime.CompareTo(((ICacheItem)y).CreationTime);
            }
        }

        void IDisposable.Dispose()
        {
            //if (lockFile != null) lockFile.Dispose();
            if (connection != null) connection.Close();
        }
    }
}
