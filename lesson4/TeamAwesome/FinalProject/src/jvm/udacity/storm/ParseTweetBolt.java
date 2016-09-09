package udacity.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import udacity.storm.tools.CountiesLookup;

/**
 * A bolt that parses the tweet into words
 */
public class ParseTweetBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;
  StringBuilder result;
  private String[] skipWords = {"rt", "to", "me","la","on","that","que",
    "followers","watch","know","not","have","like","I'm","new","good","do",
    "more","es","te","followers","Followers","las","you","and","de","my","is",
    "en","una","in","for","this","go","en","all","no","don't","up","are",
    "http","http:","https","https:","http://","https://","with","just","your",
    "para","want","your","you're","really","video","it's","when","they","their","much",
    "would","what","them","todo","FOLLOW","retweet","RETWEET","even","right","like",
    "bien","Like","will","Will","pero","Pero","can't","were","Can't","Were","TWITTER",
    "make","take","This","from","about","como","esta","follows","followed"};

  //MaxentTagger tagger ;
  CountiesLookup clookup ;
  
  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
    //tagger	= new MaxentTagger("models/english-caseless-left3words-distsim.tagger");
    clookup= new CountiesLookup();

  	 result = new StringBuilder();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // get the 1st column 'tweet' from tuple
    String tweet = tuple.getStringByField("tweet").split("DELIMITER")[0];
    double latitude = Double.parseDouble(tuple.getStringByField("tweet").split("DELIMITER")[1].split(",")[0]);
    double longitude = Double.parseDouble(tuple.getStringByField("tweet").split("DELIMITER")[1].split(",")[1]);
    String county_id = clookup.getCountyCodeByGeo(latitude, longitude);

    int sentiment = tuple.getIntegerByField("sentiment");
    
    String url = tuple.getString(0).split("DELIMITER")[2];

    
    // provide the delimiters for splitting the tweet
    String delims = "[ .,?!]+";

    String [] posTweet = PartOfSpeechTagger(tweet);
    //String [] posTweet = new String[]{"i" ,"got" ,"ebola"};
    if(posTweet != null)
    {    
    	String[] tokens = tweet.split(delims);
      	System.out.print("\tParseTweetBolt\tDEBUG:" + posTweet + ", URL: " + url + "\n");
	    // for each token/word, emit it
      	result.setLength(0);
      	int n = tokens.length;
	    for (int i = 0; i < n; i++) {
	    	if(!Arrays.asList(skipWords).contains(tokens[i])){
	    		result.append(tokens[i]); 
	   	      }
	    }
	    
	    collector.emit(new Values(tweet, result.toString(), posTweet[0], posTweet[1], posTweet[2], county_id, url, sentiment));

		
    } 

    
  }

  
  public String [] PartOfSpeechTagger(String sentence)
  {
	  String [] result = new String[3];
	  result[0] = "";
	  result[1] = "";
	  result[2] = "";
	  /*String tagged = tagger.tagString(sentence);
	  
	  System.out.println("\tParseTweetBolt\tDEBUG: tagged sentence is " + tagged);
	  
	  int seen = 0;
	  if(tagged.contains("_NN "))
	  {
		  result[0] = tagged.substring(0, tagged.indexOf("_NN"));
		  result[0] = result[0].substring(Math.max(0,result[0].lastIndexOf(" ")));
		  System.out.println("\tParseTweetBolt\tDEBUG: subject is " + result[0]);
		  seen++;
	  }
	  if(tagged.contains("_VB "))
	  {
		  result[1] = tagged.substring(0, tagged.indexOf("_VB"));
		  result[1] = result[1].substring(Math.max(0,result[1].lastIndexOf(" ")));
		  System.out.println("\tParseTweetBolt\tDEBUG: verb is " + result[1]);
		  seen++;
	  }
	  if(tagged.contains("_NNP "))
	  {
		  result[2] = tagged.substring(0, tagged.indexOf("_NNP"));
		  result[2] = result[2].substring(Math.max(0,result[2].lastIndexOf(" ")));
		  System.out.println("\tParseTweetBolt\tDEBUG: verb is " + result[2]);
		  seen++;
	  }
	  if(seen > 1 )
		  return result;
	  else
		  return null;
	  */
	  
	  
	  return result;
	  
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet-word'
    declarer.declare(new Fields("original-tweet", "tweet-word", "noun", "verb", "object", "county_id", "url", "sentiment"));
  }

}
