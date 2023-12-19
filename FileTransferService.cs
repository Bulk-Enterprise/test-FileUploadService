using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Hosting;
using System.IO;
using Microsoft.Extensions.Logging;

namespace AzureFileTransferService
{
    public class FileTransferService(ILogger<FileTransferService> logger) : BackgroundService
    {
        private readonly ILogger<FileTransferService> _logger = logger;

        private readonly string _sqlConnectionString = "Server=tcp:sql-fileupload-cus.database.windows.net,1433;Initial Catalog=sqldb-fileupload-dev;Persist Security Info=False;User ID=svcdAzureFileAPI@nationaldentex.com;Password={EnterPassword};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Authentication=Active Directory Password";
        private readonly string _blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=stfileupload555dev;AccountKey={EnterAccountKey};EndpointSuffix=core.windows.net";
        private readonly string _localFolderPath = @"\\10.11.1.134\MNFLH_WebUpload$\iTero";

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await CheckForNewFilesAndDownloadAsync();
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken); // Check every 1 minutes
            }
        }

        private async Task CheckForNewFilesAndDownloadAsync()
        {
            using (var connection = new SqlConnection(_sqlConnectionString))
            {
                await connection.OpenAsync();
                var command = new SqlCommand("SELECT TransferId, FilePath, FileName FROM Transfer WHERE Processed = 0", connection);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var transferId = reader.GetInt32(0);
                        var filePath = reader.GetString(1);
                        var fileName = reader.GetString(2);

                        bool downloadSuccessful = await DownloadFileFromBlobAsync(filePath, fileName);                        

                        if (downloadSuccessful)
                        {
                            await UpdateProcessedStatusAsync(transferId);
                        }

                        await UpdateSendTriesAsync(transferId);
                    }
                }
            }
        }

        private async Task<bool> DownloadFileFromBlobAsync(string filePath, string fileName)
        {
            try
            {
                var blobClient = new BlobClient(_blobStorageConnectionString, filePath, fileName);
                var localFilePath = Path.Combine(_localFolderPath, fileName);

                if (File.Exists(localFilePath))
                {
                    localFilePath = RenameFile(localFilePath);
                    _logger.LogInformation($"File already exists. Downloaded as new FileName: {localFilePath}");
                }

                await blobClient.DownloadToAsync(localFilePath);
                return true; 
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error downloading file from Blob Storage: {FileName}", fileName);                
                return false;
            }
        }
        private async Task UpdateProcessedStatusAsync(int transferId)
        {
            try
            {
                using (var connection = new SqlConnection(_sqlConnectionString))
                {
                    await connection.OpenAsync();
                    var command = new SqlCommand("UPDATE Transfer SET Processed = 1 WHERE TransferId = @TransferId", connection);
                    command.Parameters.AddWithValue("@TransferId", transferId);

                    await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex) 
            {
                _logger.LogError(ex, "Error updating processed status for TransferId: {TransferId}", transferId);
            }
        }

        private async Task UpdateSendTriesAsync(int transferId)
        {
            try
            {
                using (var connection = new SqlConnection(_sqlConnectionString))
                {
                    await connection.OpenAsync();
                    var command = new SqlCommand("UPDATE Transfer SET SendTries = SendTries + 1 WHERE TransferId = @TransferId", connection);
                    command.Parameters.AddWithValue("@TransferId", transferId);

                    await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating SendTries for TransferId: {TransferId}", transferId);
            }
        }

        private string RenameFile(string filePath)
        {
            var directory = Path.GetDirectoryName(filePath);
            var fileName = Path.GetFileNameWithoutExtension(filePath);
            var fileExtension = Path.GetExtension(filePath);
            var newFileName = $"{fileName}_{DateTime.Now:yyyyMMddHHmmss}{fileExtension}";
            return Path.Combine(directory, newFileName);
        }
    }
}