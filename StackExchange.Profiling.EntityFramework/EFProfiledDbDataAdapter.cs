using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using StackExchange.Profiling.Data;

namespace StackExchange.Profiling.Data
{
    public class EFProfiledDbDataAdapter : ProfiledDbDataAdapter
    {
        private IDbProfiler _curProfiler;

        /// <summary>
        /// This static variable is simply used as a non-null placeholder in the MiniProfiler.ExecuteFinish method
        /// </summary>
        private static readonly DbDataReader CurTokenReader = new DataTableReader(new DataTable());

        public EFProfiledDbDataAdapter(DbDataAdapter wrappedAdapter, IDbProfiler profiler = null)
            : base(wrappedAdapter, profiler)
        {
            _curProfiler = profiler ?? MiniProfiler.Current;
        }

        public new int Fill(DataSet dataSet, string srcTable)
        {
            var adapter = (DbDataAdapter) base.InternalAdapter;
            if (_curProfiler == null || !_curProfiler.IsActive || !(base.SelectCommand is DbCommand))
            {
                return adapter.Fill(dataSet, srcTable);
            }

            int result;
            var cmd = (DbCommand)base.SelectCommand;
            _curProfiler.ExecuteStart(cmd, ExecuteType.Reader);
            try
            {
                result = adapter.Fill(dataSet, srcTable);
            }
            catch (Exception e)
            {
                _curProfiler.OnError(cmd, ExecuteType.Reader, e);
                throw;
            }
            finally
            {
                _curProfiler.ExecuteFinish(cmd, ExecuteType.Reader, CurTokenReader);
            }

            return result;
        }

        public new int Fill(DataSet dataSet, int startRecord, int maxRecords, string srcTable)
        {
            var adapter = (DbDataAdapter)base.InternalAdapter;
            if (_curProfiler == null || !_curProfiler.IsActive || !(base.SelectCommand is DbCommand))
            {
                return adapter.Fill(dataSet, startRecord, maxRecords, srcTable);
            }

            int result;
            var cmd = (DbCommand)base.SelectCommand;
            _curProfiler.ExecuteStart(cmd, ExecuteType.Reader);
            try
            {
                result = adapter.Fill(dataSet, startRecord, maxRecords, srcTable);
            }
            catch (Exception e)
            {
                _curProfiler.OnError(cmd, ExecuteType.Reader, e);
                throw;
            }
            finally
            {
                _curProfiler.ExecuteFinish(cmd, ExecuteType.Reader, CurTokenReader);
            }

            return result;
        }

        public new int Fill(DataTable dataTable)
        {
            var adapter = (DbDataAdapter)base.InternalAdapter;
            if (_curProfiler == null || !_curProfiler.IsActive || !(base.SelectCommand is DbCommand))
            {
                return adapter.Fill(dataTable);
            }

            int result;
            var cmd = (DbCommand)base.SelectCommand;
            _curProfiler.ExecuteStart(cmd, ExecuteType.Reader);
            try
            {
                result = adapter.Fill(dataTable);
            }
            catch (Exception e)
            {
                _curProfiler.OnError(cmd, ExecuteType.Reader, e);
                throw;
            }
            finally
            {
                _curProfiler.ExecuteFinish(cmd, ExecuteType.Reader, CurTokenReader);
            }

            return result;
        }

        public new int Fill(int startRecord, int maxRecords, params DataTable[] dataTables)
        {
            var adapter = (DbDataAdapter)base.InternalAdapter;
            if (_curProfiler == null || !_curProfiler.IsActive || !(base.SelectCommand is DbCommand))
            {
                return adapter.Fill(startRecord, maxRecords, dataTables);
            }

            int result;
            var cmd = (DbCommand)base.SelectCommand;
            _curProfiler.ExecuteStart(cmd, ExecuteType.Reader);
            try
            {
                result = adapter.Fill(startRecord, maxRecords, dataTables);
            }
            catch (Exception e)
            {
                _curProfiler.OnError(cmd, ExecuteType.Reader, e);
                throw;
            }
            finally
            {
                _curProfiler.ExecuteFinish(cmd, ExecuteType.Reader, CurTokenReader);
            }

            return result;
        }
    }
}
