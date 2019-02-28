using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace Tail
{
    public class SqlServerContainer : DockerContainer
    {
        private const int HostPort = 21433;
        private const string Password = "E@syP@ssw0rd";

        private int _db;

        public SqlServerContainer()
        {
            Configuration = new SqlServerContainerConfiguration(CreateMasterConnectionStringBuilder());
        }

        private SqlConnectionStringBuilder CreateMasterConnectionStringBuilder() =>
            CreateConnectionStringBuilder("master");

        private static SqlConnectionStringBuilder CreateConnectionStringBuilder(string database) =>
            new SqlConnectionStringBuilder
            {
                DataSource = "tcp:localhost,21433",
                InitialCatalog = database,
                UserID = "sa",
                Password = Password,
                Encrypt = false,
                Enlist = false,
                IntegratedSecurity = false
            };

        public async Task<SqlConnectionStringBuilder> CreateDatabaseAsync(bool use_snapshot_isolation = true)
        {
            var database = $"DB{Interlocked.Increment(ref _db)}";
            var text = use_snapshot_isolation 
                ? $@"
CREATE DATABASE [{database}]
ALTER DATABASE [{database}] SET ALLOW_SNAPSHOT_ISOLATION ON
ALTER DATABASE [{database}] SET READ_COMMITTED_SNAPSHOT ON" 
                : $"CREATE DATABASE [{database}]";
            using (var connection = new SqlConnection(CreateMasterConnectionStringBuilder().ConnectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(text, connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
                connection.Close();
            }
            return CreateConnectionStringBuilder(database);
        }

        private class SqlServerContainerConfiguration : DockerContainerConfiguration
        {
            public SqlServerContainerConfiguration(SqlConnectionStringBuilder builder)
            {
                Image = new ImageSettings
                {
                    Registry = "mcr.microsoft.com",
                    Name = "mssql/server"
                };

                Container = new ContainerSettings
                {
                    Name = "tail-db",
                    PortBindings = new[]
                    {
                        new PortBinding
                        {
                            HostPort = HostPort,
                            GuestPort = 1433
                        }
                    },
                    EnvironmentVariables = new[]
                    {
                        "ACCEPT_EULA=Y",
                        $"SA_PASSWORD={Password}"
                    }
                };

                WaitUntilAvailable = async attempt =>
                {
                    if (attempt <= 30)
                    {
                        try
                        {
                            using (var connection = new SqlConnection(builder.ConnectionString))
                            {
                                await connection.OpenAsync();
                                connection.Close();
                            }

                            return TimeSpan.Zero;
                        }
                        catch
                        {
                        }

                        return TimeSpan.FromSeconds(1);
                    }

                    throw new TimeoutException(
                        $"The container {Container.Name} did not become available in a timely fashion.");
                };
            }
        }
    }
}
