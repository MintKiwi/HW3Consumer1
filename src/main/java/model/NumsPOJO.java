package model;

public class NumsPOJO {
    private int swiper;
    private int numlikes;
    private int numdislikes;

    public NumsPOJO(int swiper, int numlikes, int numdislikes) {
        this.swiper = swiper;
        this.numlikes = numlikes;
        this.numdislikes = numdislikes;
    }

    public int getSwiper() {
        return swiper;
    }

    public void setSwiper(int swiper) {
        this.swiper = swiper;
    }

    public int getNumlikes() {
        return numlikes;
    }

    public void setNumlikes(int numlikes) {
        this.numlikes = numlikes;
    }

    public int getNumdislikes() {
        return numdislikes;
    }

    public void setNumdislikes(int numdislikes) {
        this.numdislikes = numdislikes;
    }
}
