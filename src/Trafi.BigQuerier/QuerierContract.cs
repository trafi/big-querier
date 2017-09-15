using System;

namespace Trafi.BigQuerier
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class QuerierContract : Attribute
    {
    }
}
