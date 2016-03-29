using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Serialization;
using System.Globalization;
using CompanyX.Common;
using CompanyX.Common.Enums;
using CompanyX.Common.Helpers;
using CompanyX.Contracts;
using CompanyX.Data;
using CompanyX.Service;
using System.Data.Entity.Core.Objects;
using System.Configuration;
using System.Diagnostics;

namespace CompanyX.BL 
{
    public class TrackingProcessing
    {
        private static string _accessKey = ConfigurationManager.AppSettings["AccessKey"];
        private static int _threadSleepTimeOut = int.Parse(ConfigurationManager.AppSettings["ThreadSleepTimeOut"]);

        private static Dictionary<XmlClassLibrary.DeliveryStatusCode, CompanyXStatuses> CompanyXEventTypes = new Dictionary<XmlClassLibrary.DeliveryStatusCode, CompanyXStatuses> 
        {
            { XmlClassLibrary.DeliveryStatusCode.PickedUp, new CompanyXStatuses { Code = "TR01-AA", Description = "Some Description 1" } },
            { XmlClassLibrary.DeliveryStatusCode.Debited, new CompanyXStatuses { Code = "TR02-BB", Description = "Some Description 2" } },
            { XmlClassLibrary.DeliveryStatusCode.Departed, new CompanyXStatuses { Code = "TR02-CC", Description = "Some Description 3" } },
            { XmlClassLibrary.DeliveryStatusCode.Arrived, new CompanyXStatuses { Code = "TR03-DD", Description = "Some Description 4" } },
            { XmlClassLibrary.DeliveryStatusCode.OnLastMile, new CompanyXStatuses { Code = "TR03-EE", Description = "Some Description 5" } },
            { XmlClassLibrary.DeliveryStatusCode.Delivered, new CompanyXStatuses { Code = "TR05-FF", Description = "Some Description 6" } }
        };

        private static void ProcessTrackings()
        {
            using (var context = new CompanyXEntities())
            {
                var ordersForClosing = (from th in context.TrackingHistories
                                        join t in context.Trackings on th.DocumentNumber equals t.DocumentNumber
                                        where th.Code == XmlClassLibrary.DeliveryStatusCode.Delivered.ToString()
                                        select th.DocumentNumber).ToList();

                //Удаляем все доставленные заказы из очереди
                if (ordersForClosing.Count() > 0)
                {
                    var removeList = context.Trackings.Where(x => ordersForClosing.Contains(x.DocumentNumber));

                    if (removeList.Count() > 0)
                    {
                        context.Trackings.RemoveRange(removeList);
                        context.SaveChanges();
                    }
                }

                //Список всех заказов, которые лежат в очереди 
                var pkgListInQueue = (from tr in context.Trackings
                                      join req in context.Requests on tr.RequestId equals req.Id
                                      select new
                                      {
                                          DocumentNumber = tr.DocumentNumber,
                                          MessageNumber = tr.MessageNumber,
                                          RequestId = tr.RequestId,
                                          PkgInfDocumentNumber = req.DocumentNumber,
                                      }).Distinct().OrderBy(x => x.RequestId).ToList();

                foreach (var trackingInfo in pkgListInQueue)
                {
                    var newOrderStatuses = GetNewDeliveryStatus(trackingInfo.DocumentNumber);

                    if (newOrderStatuses == null)
                        return;

                    //Появился новый статус заказа
                    if (newOrderStatuses != null && newOrderStatuses.Count > 0)
                    {
                        //
                        var pkginf = context.Requests.FirstOrDefault(r => r.DocumentNumber == trackingInfo.PkgInfDocumentNumber && r.RequestType == MessageType.PKGINF.ToString());
                        var requestBody = pkginf.RequestBody;
                        var requestBase = (PKGINF)Helpers.GetRequest(XElement.Parse(requestBody));
                        //
                        //Уведомляем CompanyX о новом статусе

                        int sequenceNumber = 1;
                        foreach (var pkgItem in requestBase.PKGList.PKGItem)
                        {
                            var trkinf = GenerateTrkinf(trackingInfo.PkgInfDocumentNumber, trackingInfo.DocumentNumber, newOrderStatuses, pkgItem.CartonNumber, sequenceNumber);

                            if (trkinf == null)
                                return;

                            sequenceNumber++;

                            var trkinfXml = Helpers.Serialize<TRKINF>(trkinf, "http://edi.sec.CompanyX.com/GLS_ECC_LE/ELEM");

                            if (Helpers.CheckAllowedMessageTypeIdentifierForOut(trkinf) == true)
                            {
                                //Отправляем в Out в том случае, если есть интересующие CompanyX события
                                if (trkinf.EvnList.EvnItem.Count > 0)
                                {
                                    CompanyXService service = new CompanyXService();
                                    try
                                    {
                                        service.Out(trkinfXml.ToString());
                                    }
                                    catch (Exception ex)
                                    {
                                        Thread.Sleep(60000);
                                        service.Out(trkinfXml.ToString());
                                    }
                                }
                            }
                        }

                        foreach (var status in newOrderStatuses)
                        {
                            var trackingHistory = new TrackingHistory()
                            {
                                Code = status.Code.ToString(),
                                Status = status.Description,
                                DocumentNumber = trackingInfo.DocumentNumber,
                                MessageNumber = trackingInfo.MessageNumber,
                                RequestId = trackingInfo.RequestId,
                            };
                            context.TrackingHistories.Add(trackingHistory);
                        }
                        if (newOrderStatuses.Count > 0)
                        {
                            context.SaveChanges();
                        }
                    }
                }
            }
        }

        private static List<XmlClassLibrary.DeliveryStatus> GetNewDeliveryStatus(string clientsNumber)
        {
            var response = GetPegasResponse(clientsNumber);

            if (response == null)
                return null;

            var serviceList = response.OrderList.Select(x => x.ServiceList).FirstOrDefault();
            if (serviceList.Count == 0)
                return null;
            var deliveryStatuses = serviceList.Select(x => x.StatusList).FirstOrDefault().ToList();
            var deliveryStatusList = new List<XmlClassLibrary.DeliveryStatus>();

            foreach (var d in deliveryStatuses)
            {
                if (!deliveryStatusList.Select(x => x.Code).Contains(((XmlClassLibrary.DeliveryStatus)d).Code))
                    deliveryStatusList.Add((XmlClassLibrary.DeliveryStatus)d);
            }

            //TEST code. Disabled.
            #region GENERATE TEST STATUSES
            var GenerateTestStatuses = ConfigurationManager.AppSettings["GenerateTestStatuses"];

            if (GenerateTestStatuses == "True")
            {
                deliveryStatusList.Add(
                    new XmlClassLibrary.DeliveryStatus()
                    {
                        Code = XmlClassLibrary.DeliveryStatusCode.PickedUp,
                        Date = DateTime.Now,
                        Description = XmlClassLibrary.DeliveryStatusCode.PickedUp.ToString()
                    }
                );
                deliveryStatusList.Add(
                    new XmlClassLibrary.DeliveryStatus()
                    {
                        Code = XmlClassLibrary.DeliveryStatusCode.Debited,
                        Date = DateTime.Now,
                        Description = XmlClassLibrary.DeliveryStatusCode.Debited.ToString()
                    }
                );
                deliveryStatusList.Add(
                    new XmlClassLibrary.DeliveryStatus()
                    {
                        Code = XmlClassLibrary.DeliveryStatusCode.Departed,
                        Date = DateTime.Now,
                        Description = XmlClassLibrary.DeliveryStatusCode.Departed.ToString()
                    }
                );
                deliveryStatusList.Add(
                    new XmlClassLibrary.DeliveryStatus()
                    {
                        Code = XmlClassLibrary.DeliveryStatusCode.Delivered,
                        Date = DateTime.Now,
                        Description = XmlClassLibrary.DeliveryStatusCode.Delivered.ToString()
                    }
                );
            }
            //TEST code END
            #endregion

            List<string> existingStatusList = null;

            using (var context = new CompanyXEntities())
            {
                existingStatusList = context.TrackingHistories.Where(x => x.DocumentNumber == clientsNumber).Select(x => x.Code).ToList();
            }

            var ret = new List<XmlClassLibrary.DeliveryStatus>();
            ret.AddRange(deliveryStatusList);

            foreach (var item in deliveryStatusList)
            {
                if (item.Code.HasValue)
                {
                    if (existingStatusList.Contains(item.Code.Value.ToString()))
                        ret.Remove(item);
                }
            }

            return ret;
        }

        private static TRKINF GenerateTrkinf(string pkgInfdocumentNumber, string documentNumber, List<XmlClassLibrary.DeliveryStatus> statusList, string cartonId, int sequenceNumber)
        {
            TRKINF trkinf;

            using (var context = new CompanyXEntities())
            {
                var pkginf = context.Requests.FirstOrDefault(r => r.DocumentNumber == pkgInfdocumentNumber && r.RequestType == MessageType.PKGINF.ToString());
                var requestBody = pkginf.RequestBody;
                var requestBase = (PKGINF)Helpers.GetRequest(XElement.Parse(requestBody));

                var matItems = new List<MatItemForTRKINF>();

                int sequenceMatNumber = 1;

                foreach (var item in requestBase.MatList.MatItem)
                {
                    matItems.Add(new MatItemForTRKINF()
                    {
                        ChargeableWeight = item.CargoInformation.ChargeableWeight,
                        ChargeableWeightCode = item.CargoInformation.ChargeableWeightCode, //"KG"
                        GoodsDescription = "",
                        GrossWeight = item.CargoInformation.GrossWeight,
                        GrossWeightCode = item.CargoInformation.GrossWeightCode, //"KG"
                        ItemNumber = item.Material.ItemNumber,
                        Material = item.Material.MaterialName,
                        Quantity = item.CargoInformation.Quantity,
                        QuantityCode = item.CargoInformation.QuantityCode,
                        SequenceNumber = sequenceMatNumber.ToString(),
                        Volume = item.CargoInformation.Volume,
                        VolumeCode = item.CargoInformation.VolumeCode
                    });
                    sequenceMatNumber++;
                }

                var pegasResponse = GetPegasResponse(documentNumber);

                if (pegasResponse == null)
                    return null;

                DateTime? estimateDatetime = null;

                if (pegasResponse.OrderList[0] != null)
                {
                    if (pegasResponse.OrderList[0].ServiceList[0] is XmlClassLibrary.DeliveryService)
                    {
                        estimateDatetime = ((XmlClassLibrary.DeliveryService)pegasResponse.OrderList[0].ServiceList[0]).PlannedDeliveryDate;
                    }
                }

                var evnItems = new List<EvnItem>();
                int sequenceStatusNumber = 1;

                foreach (var status in statusList)
                {
                    if (status.Code.HasValue && (status.Code == XmlClassLibrary.DeliveryStatusCode.PickedUp
                            || status.Code == XmlClassLibrary.DeliveryStatusCode.Debited
                            || status.Code == XmlClassLibrary.DeliveryStatusCode.Departed
                            || status.Code == XmlClassLibrary.DeliveryStatusCode.Arrived
                            || status.Code == XmlClassLibrary.DeliveryStatusCode.OnLastMile
                            || status.Code == XmlClassLibrary.DeliveryStatusCode.Delivered))
                    {
                        evnItems.Add(new EvnItem()
                        {
                            ActualDate = status.Date.HasValue ? status.Date.Value.ToString("yyyyMMdd") : DateTime.Now.ToString("yyyyMMdd"),
                            ActualTime = status.Date.HasValue ? status.Date.Value.ToString("HHmmss") : DateTime.Now.ToString("HHmmss"),
                            CityCode = "MOW",
                            CityName = "Moscow",
                            CountryCode = "RU",
                            CountryName = "Russian Fed.",
                            CurrentEvent = "X",
                            Description = CompanyXEventTypes[status.Code.Value].Description,
                            EstimateDate = estimateDatetime.HasValue ? estimateDatetime.Value.ToString("yyyyMMdd") : "",
                            EstimateTime = estimateDatetime.HasValue ? estimateDatetime.Value.ToString("hhmmss") : "",
                            EventName = CompanyXEventTypes[status.Code.Value].Description,
                            EventType = CompanyXEventTypes[status.Code.Value].Code,
                            SequenceNumber = sequenceStatusNumber.ToString()
                        });
                        sequenceStatusNumber++;
                    }
                }

                var performersNumber = pegasResponse.OrderList.FirstOrDefault().PerformersNumber;

                //Если в новых статусах не было ни одной из интересующих CompanyX статус , то MessageFunctionCodeEnum = Original(т.е. первая отправка информации по трэкингу), 
                //в противном случае апдейтим запрос

                var notFirstDelivery = sequenceNumber > 1 || context.TrackingHistories.Where(x => x.DocumentNumber == documentNumber
                    && (x.Code == XmlClassLibrary.DeliveryStatusCode.PickedUp.ToString()
                    || x.Code == XmlClassLibrary.DeliveryStatusCode.Debited.ToString()
                    || x.Code == XmlClassLibrary.DeliveryStatusCode.Departed.ToString()
                    || x.Code == XmlClassLibrary.DeliveryStatusCode.Arrived.ToString()
                    || x.Code == XmlClassLibrary.DeliveryStatusCode.OnLastMile.ToString()
                    || x.Code == XmlClassLibrary.DeliveryStatusCode.Delivered.ToString())).Any();

                var messageFunctionCode = notFirstDelivery == true ? MessageFunctionCodeEnum.Update : MessageFunctionCodeEnum.Original;

                float volumeWeight = 0;
                float.TryParse(requestBase.TotalCargoInformation.Volume, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out volumeWeight);

                float gw = 0;
                float.TryParse(requestBase.TotalCargoInformation.GrossWeight, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out gw);

                Thread.Sleep(1000);

                trkinf = new TRKINF()
                {
                    DocumentDate = DateTime.Now.ToString("yyyyMMdd"),
                    DocumentNumber = performersNumber,
                    EvnList = new EvnList() { EvnItem = evnItems },
                    MatList = new MatListForTRKINF() { MatItem = matItems },
                    MessageFunctionCode = ((int)messageFunctionCode).ToString(),
                    MessageName = "Tracking Information",
                    MessageNumber = String.Format("TRKINF_{0}_{1}-{2}", performersNumber, DateTime.Now.ToString("hhmmss"), sequenceNumber.ToString()),
                    MessageReceiverIdentifier = "TAAA",
                    MessageReceiverName = requestBase.MessageSenderName, // "CompanyX",
                    MessageSenderIdentifier = "TAAA0000",
                    MessageSenderName = requestBase.MessageReceiverName, //"Logistic operator",
                    MessageTypeIdentifier = MessageType.TRKINF.ToString(),
                    PackingNo = requestBase.PKGList.PKGItem.Where(x => x.CartonNumber == cartonId).FirstOrDefault().CartonNumber,
                    RelatedDocumentDate = requestBase.RelatedDocumentNumber.RelatedDocumentDate,
                    RelatedDocumentNumber = requestBase.RelatedDocumentNumber.Number,
                    RelatedMessageNumber = requestBase.RelatedDocumentNumber.RelatedMessageNumber,
                    RelatedMessageTypeIdentifier = "OUTDLY",
                    TotalChargeableWeight = sequenceNumber > 1 ? "0" : volumeWeight * 167 > gw ? (Math.Ceiling((volumeWeight * 167) / 0.5) * 0.5).ToString().Replace(',', '.') : (Math.Ceiling(gw / 0.5) * 0.5).ToString().Replace(',', '.'),
                    TotalChargeableWeightCode = "KG",
                    TotalGrossWeight = requestBase.TotalCargoInformation.GrossWeight,
                    TotalGrossWeightCode = requestBase.TotalCargoInformation.GrossWeightCode,
                    TotalQuantity = requestBase.TotalCargoInformation.Quantity,
                    TotalQuantityCode = requestBase.TotalCargoInformation.QuantityCode,
                    TotalVolume = requestBase.TotalCargoInformation.Volume,
                    TotalVolumeCode = requestBase.TotalCargoInformation.VolumeCode,
                    TrackingType = "PACK"
                };
            }

            return trkinf;
        }

        private static XmlClassLibrary.Response GetPegasResponse(string clientsNumber)
        {
            Thread.Sleep(_threadSleepTimeOut);
            XmlClassLibrary.Response response = null;

            try
            {
                var orderList = new List<XmlClassLibrary.Order>();
                orderList.Add(new XmlClassLibrary.Order()
                {
                    ClientsNumber = clientsNumber
                });

                XmlClassLibrary.OrderRequest order = new XmlClassLibrary.OrderRequest();
                order.Mode = XmlClassLibrary.Enums.OrderRequestMode.Status;
                order.OrderList = orderList;

                APIService.UI_ServiceClient client = new APIService.UI_ServiceClient();
                client.ClientCredentials.Windows.ClientCredential = System.Net.CredentialCache.DefaultNetworkCredentials;

                string orderXml = XmlClassLibrary.XMLService.SerializeToXMLString(order, typeof(XmlClassLibrary.Request));

                var responseXml = client.SubmitRequest(new Guid(_accessKey), orderXml);

                response = Helpers.Deserialize<XmlClassLibrary.Response>(responseXml);
            }
            catch (Exception ex)
            {
                EventLog eventLog = new EventLog("Application");
                eventLog.Source = "Application";
                eventLog.WriteEntry(String.Format("Source: {0} Message: {1} Data: {2}", ex.Source, ex.Message, ex.Data));

                return null;
            }

            return response;
        }
    }
}
