using DynamoWebApp.Classes;
using DynamoWebApp.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;


namespace DynamoWebApp.Controllers
{
    public class HomeController : Controller
    {
        DynamoAccess da = new DynamoAccess();
        public ActionResult Index()
        {
            Home h = da.GetData();
            

            return View(h);
        }

        public ActionResult About()
        {
            ViewBag.Message = "Your application description page.";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Your contact page.";

            return View();
        }
    }
}