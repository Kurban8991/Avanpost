using Avanpost.Interviews.Task.Integration.Data.DbCommon.DbModels;
using Avanpost.Interviews.Task.Integration.Data.Models;
using Avanpost.Interviews.Task.Integration.Data.Models.Models;
using Npgsql;
using System.Text.RegularExpressions;

namespace Avanpost.Interviews.Task.Integration.SandBox.Connector
{
    public class ConnectorDb : IConnector
    {
        public void StartUp(string connectionString)
        {
            string pattern = @"(\w+)='([^']*)'";
            var con = "";
            MatchCollection matches = Regex.Matches(connectionString, pattern);

            foreach (Match match in matches)
            {
                string key = match.Groups[1].Value;
                string value = match.Groups[2].Value;

                if (key == "ConnectionString")
                {
                    con = value;
                }
            }

            var dataSourceBuilder = new NpgsqlDataSourceBuilder(con);
            var dataSource = dataSourceBuilder.Build();
            _connection = dataSource.OpenConnection(); ;
        }

        public void CreateUser(UserToCreate user)
        {
            using (var cmd = new NpgsqlCommand())
            {
                cmd.Connection = _connection;

                cmd.CommandText =
                    "INSERT INTO \"AvanpostIntegrationTestTaskSchema\".\"User\" (\"login\", \"lastName\", \"firstName\", \"middleName\", \"telephoneNumber\", \"isLead\") " +
                    "VALUES (@login, @lastName, @firstName, @middleName, @telephoneNumber, @isLead)";
                cmd.Parameters.AddWithValue("@login", user.Login);
                cmd.Parameters.AddWithValue("@lastName", "Ivan");
                cmd.Parameters.AddWithValue("@firstName", "Ivanov");
                cmd.Parameters.AddWithValue("@middleName", "Ivanovich");
                cmd.Parameters.AddWithValue("@telephoneNumber", "898989283");

                foreach (var property in user.Properties)
                {
                    cmd.Parameters.AddWithValue("@isLead", bool.Parse(property.Value));
                }

                cmd.ExecuteNonQuery();

                cmd.CommandText =
                    "INSERT INTO \"AvanpostIntegrationTestTaskSchema\".\"Passwords\" (\"userId\", password) VALUES (@login, @hashPassword)";
                cmd.Parameters.AddWithValue("@login", user.Login);
                cmd.Parameters.AddWithValue("@hashPassword", user.HashPassword);
                cmd.ExecuteNonQuery();
            }
        }

        public IEnumerable<Property> GetAllProperties()
        {
            var properties = new List<Property>();
            var command = _connection.CreateCommand();

            command.CommandText = "SELECT column_name FROM information_schema.columns" +
                                  " WHERE table_name = 'User'" +
                                  "AND (column_name = 'isLead'" +
                                  "OR column_name = 'login'" +
                                  "OR column_name = 'firstName'" +
                                  "OR column_name = 'lastName')";

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    properties.Add(new Property(reader.GetString(0), ""));
                }
            }

            command.CommandText = "SELECT column_name FROM information_schema.columns " +
                                  "WHERE table_name = 'Passwords'" +
                                  "AND column_name = 'password'";

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    properties.Add(new Property(reader.GetString(0), ""));
                }
            }

            return properties;
        }


        public IEnumerable<UserProperty> GetUserProperties(string userLogin)
        {
            var properties = new List<UserProperty>();

            string sql = @"
               SELECT 'login' as Name, login as Value FROM ""AvanpostIntegrationTestTaskSchema"".""User"" WHERE login = @userLogin
               UNION ALL
               SELECT 'lastName' as Name, ""lastName"" as Value FROM ""AvanpostIntegrationTestTaskSchema"".""User"" WHERE login = @userLogin
               UNION ALL
               SELECT 'firstName' as Name, ""firstName"" as Value FROM ""AvanpostIntegrationTestTaskSchema"".""User"" WHERE login = @userLogin
               UNION ALL
               SELECT 'middleName' as Name, ""middleName"" as Value FROM ""AvanpostIntegrationTestTaskSchema"".""User"" WHERE login = @userLogin
               UNION ALL
               SELECT 'telePhoneNumber' as Name, ""telephoneNumber"" as Value FROM ""AvanpostIntegrationTestTaskSchema"".""User"" WHERE login = @userLogin";

            using (var command = new NpgsqlCommand(sql, _connection))
            {
                command.Parameters.AddWithValue("userLogin", userLogin);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string name = reader.GetString(0);
                        string value = reader.GetString(1);
                        properties.Add(new UserProperty(name, value));
                    }
                }
            }
            return properties;
        }


        public bool IsUserExists(string userLogin)
        {
            string sql = "SELECT COUNT(*) FROM \"AvanpostIntegrationTestTaskSchema\".\"User\"" +
                         " WHERE login = @userLogin";

            using (var command = new NpgsqlCommand(sql, _connection))
            {
                command.Parameters.AddWithValue("userLogin", userLogin);
                int count = Convert.ToInt32(command.ExecuteScalar());
                return count > 0;
            }
        }

        public void UpdateUserProperties(IEnumerable<UserProperty> properties, string userLogin)
        {
            foreach (var property in properties)
            {
                string columnName = property.Name;
                string columnValue = property.Value;
                /*columnName = "telephoneNumber";*/ // В БАЗЕ ОНО С МАЛЕНЬКОЙ БУКВЫ!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                string sql =
                    $"UPDATE \"AvanpostIntegrationTestTaskSchema\".\"User\" SET \"{columnName}\" = @ColumnValue WHERE login = @Login";

                using (var cmd = new NpgsqlCommand(sql, _connection))
                {
                    cmd.Parameters.AddWithValue("ColumnValue", columnValue);
                    cmd.Parameters.AddWithValue("Login", userLogin);

                    cmd.ExecuteNonQuery();
                }
            }
        }

        public IEnumerable<Permission> GetAllPermissions()
        {
            var permissions = new List<Permission>();

            string sql = "SELECT id, name FROM \"AvanpostIntegrationTestTaskSchema\".\"RequestRight\"" +
                         " UNION " +
                         "SELECT id, name FROM \"AvanpostIntegrationTestTaskSchema\".\"ItRole\"";

            using (var cmd = new NpgsqlCommand(sql, _connection))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string id = reader["Id"].ToString();
                        string name = reader["Name"].ToString();

                        permissions.Add(new Permission(id, name, ""));
                    }
                }
            }

            return permissions;
        }

        public void AddUserPermissions(string userLogin, IEnumerable<string> rightIds)
        {
            string sql = "INSERT INTO \"AvanpostIntegrationTestTaskSchema\".\"UserITRole\" ( \"userId\", \"roleId\")" +
                         " VALUES (@userId, @rightId)";

            
            foreach (var rightId in rightIds)
            {
                using (var cmd = new NpgsqlCommand(sql, _connection))
                {
                    var rightIdValue = Regex.Match(rightId, @"\d+").Value;
                    cmd.Parameters.AddWithValue("userId", userLogin);
                    cmd.Parameters.AddWithValue("rightId", int.Parse(rightIdValue));

                    cmd.ExecuteNonQuery();
                }
            }
        }

        public void RemoveUserPermissions(string userLogin, IEnumerable<string> rightIds)
        {
            foreach (var rightId in rightIds)
            {
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = _connection;
                    cmd.CommandText = "DELETE FROM \"AvanpostIntegrationTestTaskSchema\".\"UserRequestRight\"" +
                                      " WHERE \"userId\" = @userLogin AND \"rightId\" = @rightIdValue";

                    var rightIdValue = int.Parse(Regex.Match(rightId, @"\d+").Value);
                    cmd.Parameters.AddWithValue("@userLogin", userLogin);
                    cmd.Parameters.AddWithValue("@rightIdValue", rightIdValue);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public IEnumerable<string> GetUserPermissions(string userLogin)
        {
            var userPermissions = new List<string>();
            using (var cmd = new NpgsqlCommand())
            {
                cmd.Connection = _connection;
                cmd.CommandText = "SELECT \"rightId\" FROM \"AvanpostIntegrationTestTaskSchema\".\"UserRequestRight\"" +
                                  " WHERE \"userId\" = @userId";
                cmd.Parameters.AddWithValue("@userId", userLogin);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string roleId = reader.GetInt32(0).ToString();
                        userPermissions.Add(roleId);
                    }
                }
            }

            return userPermissions;
        }
        
        private NpgsqlConnection _connection;
        public ILogger Logger { get; set; }
    }
}