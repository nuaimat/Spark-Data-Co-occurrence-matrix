package edu.mum.bigdata.spark.mo;

import java.util.Comparator;

import scala.Serializable;
import scala.Tuple2;

public class KeyComparator implements Comparator<Tuple2<String, String>>, Serializable {

		@Override
		public int compare(Tuple2<String, String> o1,
				Tuple2<String, String> o2) {
			int c1 = o1._1().compareTo(o2._1());
			if(c1 != 0)
				return c1;
			return o1._2().compareTo(o2._2());
		}
}
