using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Data;
using Amazon.Lambda.Core;
using MySql.Data.MySqlClient;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace PushAPIToSQL
{
    public class Function
    {  

        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>

        public async Task FunctionHandler(string input, ILambdaContext context)
        {
            Console.WriteLine("Atempting To Connect To SQL Server");
            //SQL
            List<long> steamIDDir = new List<long>();
            MySqlDataReader sqlData = null;
            string sqladdress = "server=sqlforstatcontrolv2-cluster.cluster-cdzwulcek08o.eu-west-2.rds.amazonaws.com;user id=admin;persistsecurityinfo=True;database=StatControlSql;Password=HowManyDucks2";
            MySqlConnection connection = new MySqlConnection(sqladdress);
            connection.Open();
            Console.WriteLine("Connected");
            MySqlCommand command = new MySqlCommand("GetSteamIDs", connection);
            command.CommandType = CommandType.StoredProcedure;
            sqlData = command.ExecuteReader();
            while (sqlData.Read())
            {
                steamIDDir.Add(Convert.ToInt64(sqlData.GetValue(0)));
            }
            connection.Close();
            Console.WriteLine("Closed Connection After Successfully Extracting Data");
            //API
            List<string[,]> WeaponsForUser = new List<string[,]>();
            string steamAPIKey = "8858FC26F97BACC3D4BB4C44CA52969F";
            string connectionURL = $"https://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v0002/?appid=730&key={ steamAPIKey }&steamid=";
            string[] responcearr;
            for (int z = 0; z < steamIDDir.Count; z++)
            {
                Console.WriteLine($"Atempting To Connect Steam API Key:{steamAPIKey} UserID:{steamIDDir[z]}");
                Console.WriteLine($"Full Steam API URL:{connectionURL + steamIDDir[z]}");
                string responce = null;
                using (var client = new WebClient())
                {
                    string url = connectionURL + steamIDDir[z].ToString();
                    responce = await client.DownloadStringTaskAsync(url);//new
                    //responce = client.DownloadString(url);//OLD Times Out Here
                    Console.WriteLine($"Successfully Connected And Retreaved Data From Steam API Key:{steamAPIKey} UserID:{steamIDDir[z]}");
                }
                if (!string.IsNullOrEmpty(responce))
                {
                    string[,] weapons = new string[,] { { steamIDDir[z].ToString(), "", "", "", "" }, { "1", "deagle", "0", "0", "0" }, { "2", "elite", "0", "0", "0" }, { "3", "fiveseven", "0", "0", "0" }, { "4", "glock", "0", "0", "0" }, { "7", "ak47", "0", "0", "0" }, { "8", "aug", "0", "0", "0" }, { "9", "awp", "0", "0", "0" }, { "10", "famas", "0", "0", "0" }, { "11", "g3sg1", "0", "0", "0" }, { "13", "galilar", "0", "0", "0" }, { "14", "m249", "0", "0", "0" }, { "16", "m4a1", "0", "0", "0" }, { "17", "mac10", "0", "0", "0" }, { "19", "p90", "0", "0", "0" }, { "24", "ump45", "0", "0", "0" }, { "25", "xm1014", "0", "0", "0" }, { "26", "bizon", "0", "0", "0" }, { "27", "mag7", "0", "0", "0" }, { "28", "negev", "0", "0", "0" }, { "29", "sawedoff", "0", "0", "0" }, { "30", "tec9", "0", "0", "0" }, { "31", "taser", "0", "0", "0" }, { "32", "hkp2000", "0", "0", "0" }, { "33", "mp7", "0", "0", "0" }, { "34", "mp9", "0", "0", "0" }, { "35", "nova", "0", "0", "0" }, { "36", "p250", "0", "0", "0" }, { "38", "scar20", "0", "0", "0" }, { "39", "sg556", "0", "0", "0" }, { "40", "ssg08", "0", "0", "0" }, { "42", "knife", "0", "0", "0" }, { "44", "hegrenade", "0", "0", "0" } };
                    responcearr = responce.Split(',');
                    for (int i = 2; i < responcearr.Length; i = i + 2)
                    {
                        string currentline = new string(responcearr[i].Where(c => char.IsLetterOrDigit(c) || char.IsWhiteSpace(c) || c == '-').ToArray());
                        string currentvalue = new string(responcearr[i + 1].Where(c => char.IsLetterOrDigit(c) || char.IsWhiteSpace(c) || c == '-').ToArray());
                        currentline = currentline.Substring(4);
                        currentvalue = currentvalue.Substring(5);
                        for (int b = 1; b < 33; b++)
                        {
                            if (currentline == $"totalkills" + weapons[b, 1])
                            {
                                weapons[b, 2] = currentvalue;
                            }
                            else if (currentline == $"totalshots" + weapons[b, 1])
                            {
                                weapons[b, 3] = currentvalue;
                            }
                            else if (currentline == $"totalhits" + weapons[b, 1])
                            {
                                weapons[b, 4] = currentvalue;
                            }
                        }
                    }
                    WeaponsForUser.Add(weapons);
                }
                else
                {
                    Console.WriteLine($"Null Data For {steamIDDir[z]}");
                }
              
            }
            //SQL
            Console.WriteLine("Attempting To Open A Connection To The SQL Server");
            connection.Open();
            Console.WriteLine("Connection Astablished");
            for (int p = 0; p < steamIDDir.Count(); p++)
            {
                string userIDForPush = WeaponsForUser[p][0, 0];
                for (int q = 1; q < 32; q++)
                {
                    string weaponid = WeaponsForUser[p][q, 0];
                    string weaponkills = WeaponsForUser[p][q, 2];
                    string weaponshots = WeaponsForUser[p][q, 3];
                    string weaponhits = WeaponsForUser[p][q, 4];
                    MySqlCommand commandtest = new MySqlCommand($"INSERT INTO WeaponData (WeaponData.SteamID,WeaponData.WeaponID,WeaponData.total_kills,WeaponData.total_shots,WeaponData.total_hits) VALUES({userIDForPush}, {weaponid}, {weaponkills}, {weaponshots}, {weaponhits})", connection);
                    commandtest.ExecuteNonQuery();                    
                }
            }
            connection.Close();
            Console.WriteLine("Closed Connection After Successfully Extracting Data");
            Console.WriteLine("All User Data Pushed To SQL App Finished Successfully :)");
            return;
        }
    }
}
