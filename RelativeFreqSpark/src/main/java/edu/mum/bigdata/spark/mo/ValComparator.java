package edu.mum.bigdata.spark.mo;

import java.util.Comparator;

import scala.Serializable;
import scala.Tuple2;

public class ValComparator implements
		Serializable, Comparator<Tuple2<Tuple2<String, String>, Float>> {

	@Override
	public int compare(Tuple2<Tuple2<String, String>, Float> o1,
			Tuple2<Tuple2<String, String>, Float> o2) {
		return -1*Float.compare(o1._2(), o2._2());
	}

}
