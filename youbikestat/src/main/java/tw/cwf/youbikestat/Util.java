package tw.cwf.youbikestat;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;

public class Util {
	public static String execGet(String targetURL) {
		String ret = "";
		HttpURLConnection connection = null;
		try {
			URL url = new URL(targetURL);
			connection = (HttpURLConnection) url.openConnection();
			int status = connection.getResponseCode();
			//System.out.println("Initial resp code: " + String.valueOf(status));
			URL newUrl = new URL(connection.getHeaderField("Location"));
			//System.out.println("Redirect to: " + newUrl.toString());
			connection = (HttpURLConnection) newUrl.openConnection();
			InputStream is = connection.getInputStream();
			GZIPInputStream gzIs = new GZIPInputStream(is);
			ret = IOUtils.toString(gzIs, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.err.println(ret);
		//System.err.println("heyheyhey!!!");
		return ret;
	}
}
