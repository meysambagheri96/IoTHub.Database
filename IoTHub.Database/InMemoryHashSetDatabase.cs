/*
High Performance Scalable In-Memory Data Structure 
- HashSet
- Tries 
- Bloom Filter 
- Term-Search O(1) 
- WildCard-Search O(1).
*/
using System.Collections.Concurrent;
using System.Text;

//Usable
public class InMemoryHashSetDatabase
{
    private Dictionary<object, HashSet<HashSet<object>>> _termIndex = new Dictionary<object, HashSet<HashSet<object>>>();
    private Dictionary<string, HashSet<HashSet<object>>> _wildcardIndex = new Dictionary<string, HashSet<HashSet<object>>>();
    private BloomFilter _bloomFilter = new BloomFilter();

    public void AddRecord(HashSet<object> record)
    {
        // Add record to term index
        foreach (object value in record)
        {
            if (!_termIndex.ContainsKey(value))
            {
                _termIndex[value] = new HashSet<HashSet<object>>();
            }
            _termIndex[value].Add(record);
        }

        // Add record to wildcard index
        foreach (object value in record)
        {
            string valueString = value.ToString();
            for (int i = 1; i <= valueString.Length; i++)
            {
                string prefix = valueString.Substring(0, i);
                if (!_wildcardIndex.ContainsKey(prefix))
                {
                    _wildcardIndex[prefix] = new HashSet<HashSet<object>>();
                }
                _wildcardIndex[prefix].Add(record);
            }
        }

        // Add record to bloom filter
        _bloomFilter.Add(record);
    }

    public IEnumerable<HashSet<object>> TermSearch(object term)
    {
        if (_termIndex.ContainsKey(term))
        {
            return _termIndex[term];
        }
        return Enumerable.Empty<HashSet<object>>();
    }

    public IEnumerable<HashSet<object>> WildcardSearch(string prefix)
    {
        if (_wildcardIndex.ContainsKey(prefix))
        {
            return _wildcardIndex[prefix];
        }
        return Enumerable.Empty<HashSet<object>>();
    }

    public bool Contains(HashSet<object> record)
    {
        return _bloomFilter.Contains(record);
    }

    public class BloomFilter
    {
        private readonly int _size;
        private readonly int _hashCount;
        private readonly bool[] _bits;

        public BloomFilter(int size = 1000000000, int hashCount = 7)
        {
            _size = size;
            _hashCount = hashCount;
            _bits = new bool[size];
        }

        public void Add(HashSet<object> record)
        {
            foreach (object value in record)
            {
                int[] hashes = GetHashes(value.ToString());
                foreach (int hash in hashes)
                {
                    _bits[hash % _size] = true;
                }
            }
        }

        public bool Contains(HashSet<object> record)
        {
            foreach (object value in record)
            {
                int[] hashes = GetHashes(value.ToString());
                foreach (int hash in hashes)
                {
                    if (!_bits[hash % _size])
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        private int[] GetHashes(string value)
        {
            int[] hashes = new int[_hashCount];
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            for (int i = 0; i < _hashCount; i++)
            {
                hashes[i] = MurmurHash3.Hash(bytes, i) & int.MaxValue;
            }
            return hashes;
        }
    }

    public static class MurmurHash3
    {
        public static int Hash(byte[] data, int seed = 0)
        {
            const uint c1 = 0xcc9e2d51;
            const uint c2 = 0x1b873593;
            const int r1 = 15;
            const int r2 = 13;
            const uint m = 5;
            const uint n = 0xe6546b64;

            int length = data.Length;
            uint h1 = (uint)seed;
            uint k1 = 0;

            int i = 0;
            while (length >= 4)
            {
                k1 = (uint)(data[i++] | data[i++] << 8 | data[i++] << 16 | data[i++] << 24);

                k1 *= c1;
                k1 = RotateLeft(k1, r1);
                k1 *= c2;

                h1 ^= k1;
                h1 = RotateLeft(h1, r2);
                h1 = h1 * m + n;

                length -= 4;
            }

            if (length > 0)
            {
                k1 = 0;
                for (int j = 0; j < length; j++)
                {
                    k1 |= (uint)data[i + j] << (8 * j);
                }

                k1 *= c1;
                k1 = RotateLeft(k1, r1);
                k1 *= c2;

                h1 ^= k1;
            }

            h1 ^= (uint)data.Length;
            h1 = Fmix(h1);

            return (int)h1;
        }

        private static uint RotateLeft(uint x, int n)
        {
            return (x << n) | (x >> (32 - n));
        }

        private static uint Fmix(uint h)
        {
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;

            return h;
        }
    }
}