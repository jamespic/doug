package uk.me.jamespic.dougng.viewmodel

/*
 * Subscription messages, for SubscribableVariables
 */
case object Subscribe
case object Unsubscribe
case class DataChanged(x: Any)