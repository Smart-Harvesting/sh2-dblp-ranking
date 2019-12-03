package de.th_koeln.iws.sh2.ranking.analysis.evaluation;

public class EvaluationResult {

	private float f;
	private float r;
	private float p;

	/**
	 * @param p
	 *            The precision
	 * @param r
	 *            The recall
	 */
	public EvaluationResult(final float p, final float r) {
		this.p = p;
		this.r = r;
		/*
		 * Wir kapseln die F-Berechnung in der Klasse (statt F aussen zu berechnen und
		 * mit reinzugeben):
		 */
		this.f = 2 * p * r / (p + r);
		/* Wenn f nicht zwischen 0 und 1 ist stimmt was nicht: */
		if (this.f < 0 || this.f > 1.0f) {
			throw new IllegalStateException("F should be between 0 and 1 but is: " + this.f);
		}
	}

	@Override
	public String toString() {
		/*
		 * In unserer toString-Darstellung formattieren wir die Zahlen auf zwei
		 * Nachkommastellen (%.2f)
		 */
		return String.format("%s with p=%.2f, r=%.2f and f=%.2f", this.getClass().getSimpleName(), this.p, this.r, this.f);
	}

	/**
	 * @return The f-measure
	 */
	public float f() {
		return this.f;
	}

	/**
	 * @return The recall
	 */
	public float r() {
		return this.r;
	}

	/**
	 * @return The precision
	 */
	public float p() {
		return this.p;
	}

}
