package exercise.processor;

import java.io.IOException;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
  
public class JsonUtil {
	
	public static final String NO_DATA = "{\"data\":null}";
	public static final String NO_RESULT = "{\"result\":false}";
	private static ObjectMapper mapper;
	static{
		mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
	}
	
	public static JsonNode josn2Object(String json){
		 try {
			return mapper.readTree(json);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block  
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block  
			e.printStackTrace();
			return null;
		}
	}
	
	public static String parseJson(Object obj){
		
		if(obj == null){
			return NO_DATA;
		}
		
		try {
			return mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block  
			e.printStackTrace();
			return NO_DATA;
		}
	}
	
	public static String parseJson(Object obj, String root){
		
		if(obj == null){
			return NO_DATA;
		}
		
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("{\"");
			sb.append(root);
			sb.append("\":");
			sb.append(mapper.writeValueAsString(obj));
			sb.append("}");
			return sb.toString();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return NO_DATA;
		}
	}
	
	public static String wrapperJsonp(String json, String var){
		if(var == null){
			var = "datas";
		}
		return new StringBuilder().append("var ").append(var).append("=").append(json).toString();
	}
	
	public static void main(String[] args) {
		HashMap<String, Integer> hash = new HashMap<String, Integer>();
		hash.put("key1", 1);
		hash.put("key2", 2);
		hash.put("key3", 3);
		System.out.println(JsonUtil.parseJson(hash));
	}
}
