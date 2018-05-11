package wtist
import scala.io.Source

/**
 * Created by wtist on 2016/6/2.
 */
class Config {
  val PropertyTable = scala.collection.mutable.HashMap[String, String]()
  val Sections = scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[String, String]]()

  def load(fileName: String): Unit = {
    val source = Source.fromFile(fileName)
    var section = ""
    for (line <- source.getLines().map(x => x.trim()) if !line.isEmpty && !line.startsWith("#") && !line.startsWith(" ")) {
      if (line.startsWith("[") && line.endsWith("]")) {
        section = line.stripPrefix("[").stripSuffix("]")
        Sections.update(section, new scala.collection.mutable.HashMap[String, String])
      }
      else if (line.contains("=")) {
        val (parameter, value) = line.split("=") match {
          case Array(parameter, value) => (parameter.trim(), value.trim())
        }
        if (!section.equals("")) {
          (Sections.get(section) match {
            case Some(section) => section
          }).getOrElseUpdate(parameter, value)
        } else {
          //TODO exception
        }
      }
    }
    init()
  }

  def sections(): scala.collection.Set[String] = {
    Sections.keySet
  }

  def options(section: String): scala.collection.Set[String] = {
    if (!Sections.contains(section)) {
      //TODO exception
    }
    val sections = Sections.get(section) match {
      case Some(sections) => sections
    }
    sections.keySet
  }

  def itmes(section: String): scala.collection.mutable.HashMap[String, String] = {
    if (!Sections.contains(section)) {
      //TODO exception
    }
    val sections = Sections.get(section) match {
      case Some(sections) => sections
    }
    sections
  }

  def itmes(): scala.collection.mutable.HashMap[String, String] = {
    PropertyTable
  }

  def build(section: String): Unit = {
    if (!Sections.contains(section)) {
      //TODO exception
    }
    PropertyTable ++= (Sections.get(section) match {
      case Some(sections) => sections
    })
  }

  def get(key: String): String = {
    PropertyTable.getOrElse(key, "Unknow")
  }

  def set(key: String, value: String): Unit ={
    PropertyTable.put(key, value)
  }

  def init(): Unit = {
    build("Common")
  }
}

object Config {
  val conf = new Config

  def apply(configure_file: String): Config = {
    val conf = new Config
    conf.load(configure_file)
    conf
  }
}

