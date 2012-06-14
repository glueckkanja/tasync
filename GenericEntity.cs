using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Data.Services.Common;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Xml;
using System.Xml.Linq;

namespace Tasync
{
    [DataServiceKey("PartitionKey", "RowKey")]
    internal class GenericEntity
    {
        private static readonly CultureInfo IC = CultureInfo.InvariantCulture;

        private static readonly XNamespace AtomNamespace =
            "http://www.w3.org/2005/Atom";

        private static readonly XNamespace AstoriaDataNamespace =
            "http://schemas.microsoft.com/ado/2007/08/dataservices";

        private static readonly XNamespace AstoriaMetadataNamespace =
            "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
        
        public GenericEntity()
        {
            Columns = new SortedDictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public SortedDictionary<string, object> Columns { get; private set; }

        internal object this[string key]
        {
            get { return Columns[key]; }
            set { Columns[key] = value; }
        }

        // http://stackoverflow.com/a/10704719/26907
        // Credit goes to Pablo from ADO.NET Data Service team 
        internal static void OnReadingEntity(object sender, ReadingWritingEntityEventArgs e)
        {
            var entity = e.Entity as GenericEntity;

            if (entity == null)
            {
                return;
            }

            // read each property, type and value in the payload   
            PropertyInfo[] properties = e.Entity.GetType().GetProperties();

            IEnumerable<XElement> elements = e.Data
                .Element(AtomNamespace + "content")
                .Element(AstoriaMetadataNamespace + "properties")
                .Elements();

            var q = from p in elements
                    where properties.All(pp => pp.Name != p.Name.LocalName)
                    select new
                               {
                                   Name = p.Name.LocalName,
                                   IsNull = String.Equals("true",p.Attribute(AstoriaMetadataNamespace + "null") == null? null: p.Attribute(AstoriaMetadataNamespace + "null").Value,StringComparison.OrdinalIgnoreCase),
                                   TypeName =p.Attribute(AstoriaMetadataNamespace + "type") == null? null: p.Attribute(AstoriaMetadataNamespace + "type").Value,
                                   p.Value
                               };

            foreach (var dp in q)
            {
                entity[dp.Name] = GetTypedEdmValue(dp.TypeName, dp.Value, dp.IsNull);
            }
        }

        internal static void OnWritingEntity(object sender, ReadingWritingEntityEventArgs e)
        {
            var entity = e.Entity as GenericEntity;

            if (entity == null)
            {
                return;
            }

            XElement root = e.Data
                .Element(AtomNamespace + "content")
                .Element(AstoriaMetadataNamespace + "properties");

            foreach (var col in entity.Columns)
            {
                var el = new XElement(AstoriaDataNamespace + col.Key);

                Tuple<object, string> tuple = GetEdmType(col.Value, col.Value == null);

                if (col.Value == null)
                {
                    el.Add(new XAttribute(AstoriaMetadataNamespace + "null", "true"));
                }
                else
                {
                    el.SetValue(tuple.Item1);
                }

                el.Add(new XAttribute(AstoriaMetadataNamespace + "type", tuple.Item2));
                
                root.Add(el);
            }
        }

        private static object GetTypedEdmValue(string type, string value, bool isnull)
        {
            if (isnull)
            {
                return null;
            }

            if (String.IsNullOrEmpty(type))
            {
                return value;
            }

            switch (type)
            {
                case "Edm.String":
                    return value;
                case "Edm.Byte":
                    return Convert.ChangeType(value, typeof (byte));
                case "Edm.SByte":
                    return Convert.ChangeType(value, typeof (sbyte));
                case "Edm.Int16":
                    return Convert.ChangeType(value, typeof (short));
                case "Edm.Int32":
                    return Convert.ChangeType(value, typeof (int));
                case "Edm.Int64":
                    return Convert.ChangeType(value, typeof (long));
                case "Edm.Double":
                    return Convert.ChangeType(value, typeof (double), IC);
                case "Edm.Single":
                    return Convert.ChangeType(value, typeof (float), IC);
                case "Edm.Boolean":
                    return Convert.ChangeType(value, typeof (bool));
                case "Edm.Decimal":
                    return Convert.ChangeType(value, typeof (decimal), IC);
                case "Edm.DateTime":
                    return XmlConvert.ToDateTime(value, XmlDateTimeSerializationMode.RoundtripKind);
                case "Edm.Binary":
                    return Convert.FromBase64String(value);
                case "Edm.Guid":
                    return new Guid(value);

                default:
                    throw new NotSupportedException("Not supported type " + type);
            }
        }

        private static Tuple<object, string> GetEdmType(object value, bool isnull)
        {
            if (isnull)
            {
                return null;
            }

            if (value is string) return Tuple.Create(Convert.ChangeType(value, typeof (string)), "Edm.String");
            if (value is byte) return Tuple.Create(Convert.ChangeType(value, typeof (string)), "Edm.Byte");
            if (value is sbyte) return Tuple.Create(Convert.ChangeType(value, typeof (string)), "Edm.SByte");
            if (value is short) return Tuple.Create(Convert.ChangeType(value, typeof (string)), "Edm.Int16");
            if (value is int) return Tuple.Create(Convert.ChangeType(value, typeof (string)), "Edm.Int32");
            if (value is long) return Tuple.Create(Convert.ChangeType(value, typeof (string)), "Edm.Int64");
            if (value is double) return Tuple.Create(Convert.ChangeType(value, typeof (string), IC), "Edm.Double");
            if (value is float) return Tuple.Create(Convert.ChangeType(value, typeof (string), IC), "Edm.Single");
            if (value is bool) return Tuple.Create((object) value.ToString().ToLowerInvariant(), "Edm.Boolean");
            if (value is decimal) return Tuple.Create(Convert.ChangeType(value, typeof (string), IC), "Edm.Decimal");
            if (value is DateTime) return Tuple.Create((object) ((DateTime) value).ToString("o"), "Edm.DateTime");
            if (value is byte[]) return Tuple.Create((object) Convert.ToBase64String((byte[]) value), "Edm.Binary");
            if (value is Guid) return Tuple.Create((object) value.ToString(), "Edm.Guid");

            throw new NotSupportedException("Not supported type " + value.GetType().FullName);
        }
    }
}