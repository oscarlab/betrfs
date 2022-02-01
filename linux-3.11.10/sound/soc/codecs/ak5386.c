/*
 * ALSA SoC driver for
 *    Asahi Kasei AK5386 Single-ended 24-Bit 192kHz delta-sigma ADC
 *
 * (c) 2013 Daniel Mack <zonque@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

#include <linux/module.h>
#include <linux/slab.h>
#include <linux/of.h>
#include <linux/of_gpio.h>
#include <linux/of_device.h>
#include <sound/soc.h>
#include <sound/pcm.h>
#include <sound/initval.h>

struct ak5386_priv {
	int reset_gpio;
};

static struct snd_soc_codec_driver soc_codec_ak5386;

static int ak5386_set_dai_fmt(struct snd_soc_dai *codec_dai,
			      unsigned int format)
{
	struct snd_soc_codec *codec = codec_dai->codec;

	format &= SND_SOC_DAIFMT_FORMAT_MASK;
	if (format != SND_SOC_DAIFMT_LEFT_J &&
	    format != SND_SOC_DAIFMT_I2S) {
		dev_err(codec->dev, "Invalid DAI format\n");
		return -EINVAL;
	}

	return 0;
}

static int ak5386_hw_params(struct snd_pcm_substream *substream,
			    struct snd_pcm_hw_params *params,
			    struct snd_soc_dai *dai)
{
	struct snd_soc_codec *codec = dai->codec;
	struct ak5386_priv *priv = snd_soc_codec_get_drvdata(codec);

	/*
	 * From the datasheet:
	 *
	 * All external clocks (MCLK, SCLK and LRCK) must be present unless
	 * PDN pin = “L”. If these clocks are not provided, the AK5386 may
	 * draw excess current due to its use of internal dynamically
	 * refreshed logic. If the external clocks are not present, place
	 * the AK5386 in power-down mode (PDN pin = “L”).
	 */

	if (gpio_is_valid(priv->reset_gpio))
		gpio_set_value(priv->reset_gpio, 1);

	return 0;
}

static int ak5386_hw_free(struct snd_pcm_substream *substream,
			  struct snd_soc_dai *dai)
{
	struct snd_soc_codec *codec = dai->codec;
	struct ak5386_priv *priv = snd_soc_codec_get_drvdata(codec);

	if (gpio_is_valid(priv->reset_gpio))
		gpio_set_value(priv->reset_gpio, 0);

	return 0;
}

static const struct snd_soc_dai_ops ak5386_dai_ops = {
	.set_fmt	= ak5386_set_dai_fmt,
	.hw_params	= ak5386_hw_params,
	.hw_free	= ak5386_hw_free,
};

static struct snd_soc_dai_driver ak5386_dai = {
	.name		= "ak5386-hifi",
	.capture	= {
		.stream_name	= "Capture",
		.channels_min	= 1,
		.channels_max	= 2,
		.rates		= SNDRV_PCM_RATE_8000_192000,
		.formats	= SNDRV_PCM_FMTBIT_S8     |
				  SNDRV_PCM_FMTBIT_S16_LE |
				  SNDRV_PCM_FMTBIT_S24_LE |
				  SNDRV_PCM_FMTBIT_S24_3LE,
	},
	.ops	= &ak5386_dai_ops,
};

#ifdef CONFIG_OF
static const struct of_device_id ak5386_dt_ids[] = {
	{ .compatible = "asahi-kasei,ak5386", },
	{ }
};
MODULE_DEVICE_TABLE(of, ak5386_dt_ids);
#endif

static int ak5386_probe(struct platform_device *pdev)
{
	struct device *dev = &pdev->dev;
	struct ak5386_priv *priv;

	priv = devm_kzalloc(dev, sizeof(*priv), GFP_KERNEL);
	if (!priv)
		return -ENOMEM;

	priv->reset_gpio = -EINVAL;
	dev_set_drvdata(dev, priv);

	if (of_match_device(of_match_ptr(ak5386_dt_ids), dev))
		priv->reset_gpio = of_get_named_gpio(dev->of_node,
						      "reset-gpio", 0);

	if (gpio_is_valid(priv->reset_gpio))
		if (devm_gpio_request_one(dev, priv->reset_gpio,
					  GPIOF_OUT_INIT_LOW,
					  "AK5386 Reset"))
			priv->reset_gpio = -EINVAL;

	return snd_soc_register_codec(dev, &soc_codec_ak5386,
				      &ak5386_dai, 1);
}

static int ak5386_remove(struct platform_device *pdev)
{
	snd_soc_unregister_codec(&pdev->dev);
	return 0;
}

static struct platform_driver ak5386_driver = {
	.probe		= ak5386_probe,
	.remove		= ak5386_remove,
	.driver		= {
		.name	= "ak5386",
		.owner	= THIS_MODULE,
		.of_match_table = of_match_ptr(ak5386_dt_ids),
	},
};

module_platform_driver(ak5386_driver);

MODULE_DESCRIPTION("ASoC driver for AK5386 ADC");
MODULE_AUTHOR("Daniel Mack <zonque@gmail.com>");
MODULE_LICENSE("GPL");
